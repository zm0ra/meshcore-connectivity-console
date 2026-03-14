from __future__ import annotations

import asyncio
import logging
import os
import struct
from datetime import UTC, datetime
from dataclasses import asdict
from typing import Any, cast

from .config import AppConfig, EndpointConfig
from .database import BotDatabase
from .identity import LocalIdentity
from .mesh_builders import build_login_packet, build_request_packet, next_request_tag, parse_encrypted_datagram, parse_path_response
from .mesh_builders import build_advert_packet
from .mesh_packets import AdvertType, PayloadType, RouteType, describe_packet_summary
from .repeater_protocol import (
    REQ_TYPE_GET_NEIGHBOURS,
    REQ_TYPE_GET_OWNER_INFO,
    REQ_TYPE_GET_STATUS,
    RESP_SERVER_LOGIN_OK,
    parse_login_response,
    parse_neighbours_response,
    parse_owner_info_response,
    parse_status_response,
)
from .tcp_client import MeshcoreTCPClient, ReceivedPacket


class ProbeTimeoutError(TimeoutError):
    pass


def select_login_candidates(*, config, remote_pubkey: bytes, repeater_name: str | None) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    pubkey_hex = remote_pubkey.hex().upper()
    normalized_name = (repeater_name or "").strip().upper()

    if config.admin_password and (
        any(pubkey_hex.startswith(prefix) for prefix in config.admin_password_pubkey_prefixes)
        or any(normalized_name.startswith(prefix.upper()) for prefix in config.admin_password_name_prefixes)
    ):
        candidates.append(("admin", config.admin_password))

    if config.guest_password and (
        any(pubkey_hex.startswith(prefix) for prefix in config.guest_password_pubkey_prefixes)
        or any(normalized_name.startswith(prefix.upper()) for prefix in config.guest_password_name_prefixes)
    ):
        candidates.append(("guest", config.guest_password))

    if config.default_guest_password == "" or config.default_guest_password or not candidates:
        candidates.append(("guest", config.default_guest_password))

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


class GuestProbeWorker:
    def __init__(self, config: AppConfig, database: BotDatabase) -> None:
        self.config = config
        self.database = database
        probe_key_path = config.probe.key_file_path or config.identity.key_file_path
        self.identity = LocalIdentity.load_or_create(probe_key_path)
        self.logger = logging.getLogger(f"{config.service.name}.probe")
        self._stop_event = asyncio.Event()
        self._endpoint_map = {endpoint.name: endpoint for endpoint in config.endpoints if endpoint.enabled}
        self._local_hash = self.identity.public_hash(1)

    async def run(self) -> None:
        self.database.initialize()
        recovered = self.database.recover_interrupted_probe_work()
        if recovered["jobs_requeued"] or recovered["runs_interrupted"]:
            self.logger.warning(
                "recovered interrupted probe work jobs=%s runs=%s",
                recovered["jobs_requeued"],
                recovered["runs_interrupted"],
            )
        while not self._stop_event.is_set():
            job = self.database.claim_probe_job()
            if job is None:
                await asyncio.sleep(self.config.probe.poll_interval_secs)
                continue
            await self._run_job(job)

    async def stop(self) -> None:
        self._stop_event.set()

    async def _run_job(self, job: dict[str, object]) -> None:
        job_id = int(cast(int, job["id"]))
        endpoint_name = str(cast(str, job["endpoint_name"]))
        endpoint = self._endpoint_map.get(endpoint_name)
        if endpoint is None:
            self.database.finish_probe_job(job_id, status="failed", last_error=f"unknown endpoint {endpoint_name}")
            return

        repeater_id = int(cast(int, job["repeater_id"]))
        remote_pubkey = bytes(cast(bytes, job["pubkey"]))
        repeater_name = cast(str | None, job.get("last_name_from_advert"))
        probe_run_id = self.database.create_probe_run(repeater_id=repeater_id, endpoint_name=endpoint.name)

        try:
            await self.probe_repeater_as_guest(
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                endpoint=endpoint,
                remote_pubkey=remote_pubkey,
                repeater_name=repeater_name,
            )
        except Exception as exc:
            self.logger.warning("probe job %s failed: %s", job_id, exc)
            self.database.complete_probe_run(
                probe_run_id,
                repeater_id=repeater_id,
                result="failed",
                guest_login_ok=False,
                guest_permissions=None,
                firmware_capability_level=None,
                login_server_time=None,
                error_message=str(exc),
            )
            self.database.finish_probe_job(job_id, status="failed", last_error=str(exc))
            return

        self.database.finish_probe_job(job_id, status="completed")

    async def probe_repeater_as_guest(
        self,
        *,
        probe_run_id: int,
        repeater_id: int,
        endpoint: EndpointConfig,
        remote_pubkey: bytes,
        repeater_name: str | None,
    ) -> None:
        shared_secret = self.identity.calc_shared_secret(remote_pubkey)
        client = MeshcoreTCPClient(endpoint.raw_host, endpoint.raw_port)
        learned_path_len = 0
        learned_path_bytes = b""
        guest_permissions: int | None = None
        firmware_capability_level: int | None = None
        login_server_time: int | None = None
        login_candidates = select_login_candidates(
            config=self.config.probe,
            remote_pubkey=remote_pubkey,
            repeater_name=repeater_name,
        )
        latest_path = self.database.latest_repeater_path(repeater_id=repeater_id)
        if latest_path is not None and not self._is_usable_stored_path(latest_path):
            latest_path = None
        if latest_path is None:
            latest_path = self.database.latest_repeater_advert_path(repeater_id=repeater_id)
        if latest_path is not None and not self._is_usable_stored_path(latest_path):
            latest_path = None
        if latest_path is not None:
            learned_path_len = int(cast(int, latest_path.get("out_path_len", latest_path.get("path_len"))))
            learned_path_bytes = bytes.fromhex(str(cast(str, latest_path.get("out_path_hex", latest_path.get("path_hex")))))

        await client.connect()
        try:
            if self.config.probe.pre_login_advert_name:
                advert_packet = build_advert_packet(
                    identity=self.identity,
                    name=self.config.probe.pre_login_advert_name,
                    advert_type=int(AdvertType.CHAT),
                )
                advert_frame_hex = await client.send_packet(advert_packet.packet)
                self.database.insert_raw_packet(
                    probe_run_id=probe_run_id,
                    endpoint_name=endpoint.name,
                    observed_at=datetime.now(tz=UTC).isoformat(),
                    direction="tx",
                    transport="rs232bridge",
                    rs232_frame_hex=advert_frame_hex,
                    mesh_packet_hex=advert_packet.packet.hex().upper(),
                    payload_type=int(advert_packet.summary.payload_type),
                    route_type=int(advert_packet.summary.route_type),
                    remote_pubkey_hex=remote_pubkey.hex().upper(),
                    notes=f"pre-login advert name={self.config.probe.pre_login_advert_name}",
                )
                self.logger.info(
                    "decoded tx frame endpoint=%s repeater=%s name=%s %s notes=%s frame=%s packet=%s",
                    endpoint.name,
                    remote_pubkey.hex().upper()[:12],
                    (repeater_name or "").strip() or "-",
                    describe_packet_summary(advert_packet.summary),
                    f"pre-login advert name={self.config.probe.pre_login_advert_name}",
                    advert_frame_hex,
                    advert_packet.packet.hex().upper(),
                )
                if self.config.probe.pre_login_advert_delay_secs > 0:
                    await asyncio.sleep(self.config.probe.pre_login_advert_delay_secs)

            login_payload = b""
            login_error: Exception | None = None
            is_szn_direct = (repeater_name or "").strip().upper().startswith("SZN_")
            for login_role, login_password in login_candidates:
                route_attempts = [(0, b"")]
                if learned_path_len and learned_path_bytes:
                    route_attempts = [(learned_path_len, learned_path_bytes)]
                    if not is_szn_direct:
                        route_attempts.append((0, b""))
                for route_path_len, route_path_bytes in route_attempts:
                    password_label = "empty" if login_password == "" else "configured"
                    route_label = "direct" if route_path_len else "flood"
                    login_packet = build_login_packet(
                        identity=self.identity,
                        remote_public_key=remote_pubkey,
                        guest_password=login_password,
                        encoded_path_len=route_path_len,
                        path_bytes=route_path_bytes,
                    )
                    frame_hex = await client.send_packet(login_packet.packet)
                    self.database.insert_raw_packet(
                        probe_run_id=probe_run_id,
                        endpoint_name=endpoint.name,
                        observed_at=datetime.now(tz=UTC).isoformat(),
                        direction="tx",
                        transport="rs232bridge",
                        rs232_frame_hex=frame_hex,
                        mesh_packet_hex=login_packet.packet.hex().upper(),
                        payload_type=int(login_packet.summary.payload_type),
                        route_type=int(login_packet.summary.route_type),
                        remote_pubkey_hex=remote_pubkey.hex().upper(),
                        notes=f"{login_role} login route={route_label} password={password_label}",
                    )
                    self.logger.info(
                        "decoded tx frame endpoint=%s repeater=%s name=%s role=%s route=%s password=%s %s notes=%s frame=%s packet=%s",
                        endpoint.name,
                        remote_pubkey.hex().upper()[:12],
                        (repeater_name or "").strip() or "-",
                        login_role,
                        route_label,
                        password_label,
                        describe_packet_summary(login_packet.summary),
                        f"{login_role} login route={route_label} password={password_label}",
                        frame_hex,
                        login_packet.packet.hex().upper(),
                    )

                    try:
                        login_payload, login_path_len, login_path_bytes = await self._await_login_response(
                            client=client,
                            endpoint_name=endpoint.name,
                            probe_run_id=probe_run_id,
                            remote_pubkey=remote_pubkey,
                            shared_secret=shared_secret,
                        )
                        if login_path_len:
                            learned_path_len = login_path_len
                            learned_path_bytes = login_path_bytes
                        elif not learned_path_len and route_path_len:
                            learned_path_len = route_path_len
                            learned_path_bytes = route_path_bytes
                        break
                    except ProbeTimeoutError as exc:
                        login_error = exc
                        self.logger.warning(
                            "login attempt failed endpoint=%s repeater=%s role=%s route=%s error=%s",
                            endpoint.name,
                            remote_pubkey.hex().upper()[:12],
                            login_role,
                            route_label,
                            exc,
                        )
                        continue
                else:
                    continue
                break
            else:
                assert login_error is not None
                raise login_error

            login = parse_login_response(login_payload)
            if login.response_code != RESP_SERVER_LOGIN_OK:
                raise RuntimeError(f"guest login rejected with code {login.response_code}")
            guest_permissions = login.permissions
            firmware_capability_level = login.firmware_capability_level
            login_server_time = login.server_time
            if learned_path_len:
                self.database.save_repeater_path(
                    repeater_id=repeater_id,
                    encoded_path_len=learned_path_len,
                    path_hex=learned_path_bytes.hex().upper(),
                    source="login_response_path",
                )
            learned_path_len, learned_path_bytes = await self._settle_post_login_frames(
                client=client,
                endpoint_name=endpoint.name,
                probe_run_id=probe_run_id,
                repeater_id=repeater_id,
                remote_pubkey=remote_pubkey,
                shared_secret=shared_secret,
                current_path_len=learned_path_len,
                current_path_bytes=learned_path_bytes,
            )

            neighbour_pages_saved = 0
            offset = 0
            while True:
                neighbours_tag = next_request_tag()
                neighbours_plaintext = (
                    struct.pack("<I", neighbours_tag)
                    + bytes([
                        REQ_TYPE_GET_NEIGHBOURS,
                        0,
                        self.config.probe.neighbours_page_size,
                    ])
                    + struct.pack("<H", offset)
                    + bytes([
                        0,
                        self.config.probe.neighbours_prefix_len,
                    ])
                )
                neighbours_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=neighbours_plaintext,
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                await self._send_and_record(endpoint.name, probe_run_id, remote_pubkey, client, neighbours_request, neighbours_tag, f"get_neighbours offset={offset}")
                neighbours_payload, learned_path_len, learned_path_bytes = await self._await_tagged_response(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    expected_tag=neighbours_tag,
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                )
                neighbours = parse_neighbours_response(
                    neighbours_payload,
                    pubkey_prefix_len=self.config.probe.neighbours_prefix_len,
                )
                self.database.save_neighbour_snapshot_page(
                    probe_run_id=probe_run_id,
                    page_offset=offset,
                    total_neighbours_count=neighbours.neighbours_count,
                    results_count=neighbours.results_count,
                    entries=[
                        {
                            "neighbour_pubkey_prefix_hex": entry.pubkey_prefix_hex,
                            "heard_seconds_ago": entry.heard_seconds_ago,
                            "snr": entry.snr,
                        }
                        for entry in neighbours.entries
                    ],
                )
                neighbour_pages_saved += 1
                offset += neighbours.results_count
                if neighbours.results_count == 0 or offset >= neighbours.neighbours_count:
                    break

            if neighbour_pages_saved == 0:
                raise RuntimeError("neighbours polling returned no pages")

            try:
                status_tag = next_request_tag()
                status_plaintext = struct.pack("<IB4s4s", status_tag, REQ_TYPE_GET_STATUS, b"\x00\x00\x00\x00", os.urandom(4))
                status_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=status_plaintext,
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                await self._send_and_record(endpoint.name, probe_run_id, remote_pubkey, client, status_request, status_tag, "get_status")
                status_payload, learned_path_len, learned_path_bytes = await self._await_tagged_response(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    expected_tag=status_tag,
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                )
                status = parse_status_response(status_payload)
                self.database.save_status_snapshot(probe_run_id=probe_run_id, status=asdict(status))
            except Exception as exc:
                self.logger.warning("optional status polling failed for repeater %s: %s", remote_pubkey.hex().upper()[:12], exc)

            try:
                owner_tag = next_request_tag()
                owner_request = build_request_packet(
                    identity=self.identity,
                    remote_public_key=remote_pubkey,
                    plaintext=struct.pack("<I", owner_tag) + bytes([REQ_TYPE_GET_OWNER_INFO]),
                    encoded_path_len=learned_path_len,
                    path_bytes=learned_path_bytes,
                )
                await self._send_and_record(endpoint.name, probe_run_id, remote_pubkey, client, owner_request, owner_tag, "get_owner_info")
                owner_payload, learned_path_len, learned_path_bytes = await self._await_tagged_response(
                    client=client,
                    endpoint_name=endpoint.name,
                    probe_run_id=probe_run_id,
                    repeater_id=repeater_id,
                    remote_pubkey=remote_pubkey,
                    shared_secret=shared_secret,
                    expected_tag=owner_tag,
                    current_path_len=learned_path_len,
                    current_path_bytes=learned_path_bytes,
                )
                owner = parse_owner_info_response(owner_payload)
                self.database.save_owner_snapshot(
                    probe_run_id=probe_run_id,
                    firmware_version=owner.firmware_version,
                    node_name=owner.node_name,
                    owner_info=owner.owner_info,
                )
            except Exception as exc:
                self.logger.warning("optional owner polling failed for repeater %s: %s", remote_pubkey.hex().upper()[:12], exc)

            self.database.complete_probe_run(
                probe_run_id,
                repeater_id=repeater_id,
                result="success",
                guest_login_ok=True,
                guest_permissions=guest_permissions,
                firmware_capability_level=firmware_capability_level,
                login_server_time=login_server_time,
                error_message=None,
            )
        finally:
            await client.close()

    async def _send_and_record(self, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, client: MeshcoreTCPClient, packet, request_tag: int, notes: str) -> None:
        frame_hex = await client.send_packet(packet.packet)
        self.database.insert_raw_packet(
            probe_run_id=probe_run_id,
            endpoint_name=endpoint_name,
            observed_at=datetime.now(tz=UTC).isoformat(),
            direction="tx",
            transport="rs232bridge",
            rs232_frame_hex=frame_hex,
            mesh_packet_hex=packet.packet.hex().upper(),
            payload_type=int(packet.summary.payload_type),
            route_type=int(packet.summary.route_type),
            remote_pubkey_hex=remote_pubkey.hex().upper(),
            request_tag=request_tag,
            notes=notes,
        )
        self.logger.info(
            "decoded tx frame endpoint=%s repeater=%s %s tag=%s notes=%s frame=%s packet=%s",
            endpoint_name,
            remote_pubkey.hex().upper()[:12],
            describe_packet_summary(packet.summary),
            request_tag,
            notes,
            frame_hex,
            packet.packet.hex().upper(),
        )

    async def _await_login_response(self, *, client: MeshcoreTCPClient, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, shared_secret: bytes) -> tuple[bytes, int, bytes]:
        deadline = asyncio.get_running_loop().time() + self.config.probe.request_timeout_secs
        remote_hash = remote_pubkey[:1]
        last_observation = "none"
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise ProbeTimeoutError(f"timeout waiting for login response; last_observation={last_observation}")
            try:
                received = await client.receive_packet(timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise ProbeTimeoutError(f"timeout waiting for login response; last_observation={last_observation}") from exc
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.ANON_REQ and len(summary.payload) >= 33:
                sender_public_key = summary.payload[1:33]
                if sender_public_key == self.identity.public_key:
                    last_observation = "echoed-own-anon-req"
                    self.logger.info("ignored echoed own login anon request")
                    continue
            if summary.payload_type is PayloadType.PATH:
                try:
                    path_response = parse_path_response(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"path-decrypt-failed:{exc}"
                    self.logger.info("ignored login candidate path frame reason=%s", exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=path_response.source_hash,
                    destination_hash=path_response.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-path"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored login PATH from foreign hashes src=%s dst=%s",
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = (
                    "path"
                    f":extra_type={path_response.extra_type}"
                    f":path_len={path_response.encoded_path_len & 0x3F}"
                    f":src={path_response.source_hash.hex().upper()}"
                    f":dst={path_response.destination_hash.hex().upper()}"
                )
                if path_response.extra_type == int(PayloadType.RESPONSE):
                    self.logger.info(
                        "accepted login response via PATH src=%s dst=%s path_len=%s",
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                        path_response.encoded_path_len & 0x3F,
                    )
                    return path_response.extra_payload, path_response.encoded_path_len, path_response.path_bytes
                self.logger.info(
                    "ignored login PATH frame extra_type=%s src=%s dst=%s",
                    path_response.extra_type,
                    path_response.source_hash.hex().upper(),
                    path_response.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is PayloadType.RESPONSE:
                try:
                    decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                    parse_login_response(decrypted.plaintext)
                except Exception as exc:
                    last_observation = f"response-decrypt-failed:{exc}"
                    self.logger.info("ignored login candidate response reason=%s", exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=decrypted.source_hash,
                    destination_hash=decrypted.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-response"
                        f":src={decrypted.source_hash.hex().upper()}"
                        f":dst={decrypted.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored login RESPONSE from foreign hashes src=%s dst=%s",
                        decrypted.source_hash.hex().upper(),
                        decrypted.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = (
                    "response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "accepted login response via RESPONSE src=%s dst=%s",
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                return decrypted.plaintext, 0, b""
            last_observation = f"ignored-payload-type:{summary.payload_type.name}"
            self.logger.info("ignored login frame payload_type=%s", summary.payload_type.name)

    async def _settle_post_login_frames(
        self,
        *,
        client: MeshcoreTCPClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        current_path_len: int,
        current_path_bytes: bytes,
    ) -> tuple[int, bytes]:
        deadline = asyncio.get_running_loop().time() + 1.5
        learned_path_len = current_path_len
        learned_path_bytes = current_path_bytes
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return learned_path_len, learned_path_bytes
            try:
                received = await client.receive_packet(timeout=min(remaining, 0.25))
            except asyncio.TimeoutError:
                continue
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.ANON_REQ and len(summary.payload) >= 33:
                sender_public_key = summary.payload[1:33]
                if sender_public_key == self.identity.public_key:
                    continue
            if summary.payload_type is not PayloadType.PATH:
                continue
            try:
                path_response = parse_path_response(summary, shared_secret=shared_secret)
            except Exception:
                continue
            if path_response.extra_type != int(PayloadType.RESPONSE):
                continue
            try:
                parse_login_response(path_response.extra_payload)
            except Exception:
                continue
            if path_response.encoded_path_len:
                learned_path_len = path_response.encoded_path_len
                learned_path_bytes = path_response.path_bytes
                self.database.save_repeater_path(
                    repeater_id=repeater_id,
                    encoded_path_len=learned_path_len,
                    path_hex=learned_path_bytes.hex().upper(),
                    source="login_response_path",
                )

    async def _await_tagged_response(
        self,
        *,
        client: MeshcoreTCPClient,
        endpoint_name: str,
        probe_run_id: int,
        repeater_id: int,
        remote_pubkey: bytes,
        shared_secret: bytes,
        expected_tag: int,
        current_path_len: int,
        current_path_bytes: bytes,
    ) -> tuple[bytes, int, bytes]:
        deadline = asyncio.get_running_loop().time() + self.config.probe.request_timeout_secs
        remote_hash = remote_pubkey[:1]
        last_observation = "none"
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise ProbeTimeoutError(
                    f"timeout waiting for tagged response tag={expected_tag}; last_observation={last_observation}"
                )
            try:
                received = await client.receive_packet(timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise ProbeTimeoutError(
                    f"timeout waiting for tagged response tag={expected_tag}; last_observation={last_observation}"
                ) from exc
            self._record_rx(endpoint_name, probe_run_id, remote_pubkey, received)
            summary = received.summary
            if summary.payload_type is PayloadType.PATH:
                try:
                    path_response = parse_path_response(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"path-decrypt-failed:{exc}"
                    self.logger.info("ignored tagged PATH frame tag=%s reason=%s", expected_tag, exc)
                    continue
                if not self._is_remote_to_local_datagram(
                    source_hash=path_response.source_hash,
                    destination_hash=path_response.destination_hash,
                    remote_hash=remote_hash,
                ):
                    last_observation = (
                        "foreign-path"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored tagged PATH from foreign hashes tag=%s src=%s dst=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if path_response.encoded_path_len:
                    current_path_len, current_path_bytes = self._save_repeater_path_update(
                        repeater_id=repeater_id,
                        encoded_path_len=path_response.encoded_path_len,
                        path_bytes=path_response.path_bytes,
                        source="path_update",
                    )
                if path_response.extra_type != int(PayloadType.RESPONSE):
                    last_observation = (
                        f"path-extra-type={path_response.extra_type}"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored tagged PATH frame tag=%s extra_type=%s src=%s dst=%s",
                        expected_tag,
                        path_response.extra_type,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if self._is_login_response_payload(path_response.extra_payload):
                    last_observation = (
                        "late-login-response"
                        f":src={path_response.source_hash.hex().upper()}"
                        f":dst={path_response.destination_hash.hex().upper()}"
                    )
                    self.logger.info(
                        "ignored late login PATH while waiting for tag=%s src=%s dst=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                    )
                    continue
                if len(path_response.extra_payload) >= 4 and struct.unpack_from("<I", path_response.extra_payload, 0)[0] == expected_tag:
                    current_path_len, current_path_bytes = self._save_repeater_path_update(
                        repeater_id=repeater_id,
                        encoded_path_len=path_response.encoded_path_len,
                        path_bytes=path_response.path_bytes,
                        source="response_path",
                    )
                    self.logger.info(
                        "accepted tagged PATH response tag=%s src=%s dst=%s path_len=%s",
                        expected_tag,
                        path_response.source_hash.hex().upper(),
                        path_response.destination_hash.hex().upper(),
                        path_response.encoded_path_len & 0x3F,
                    )
                    return path_response.extra_payload, current_path_len, current_path_bytes
                path_tag = struct.unpack_from("<I", path_response.extra_payload, 0)[0] if len(path_response.extra_payload) >= 4 else None
                last_observation = (
                    f"path-tag-mismatch={path_tag}"
                    f":src={path_response.source_hash.hex().upper()}"
                    f":dst={path_response.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored tagged PATH response expected_tag=%s actual_tag=%s src=%s dst=%s",
                    expected_tag,
                    path_tag,
                    path_response.source_hash.hex().upper(),
                    path_response.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is PayloadType.REQ:
                try:
                    decrypted_req = parse_encrypted_datagram(summary, shared_secret=shared_secret)
                except Exception as exc:
                    last_observation = f"req-decrypt-failed:{exc}"
                    self.logger.info("ignored req frame while waiting for tag=%s reason=%s", expected_tag, exc)
                    continue
                request_tag = struct.unpack_from("<I", decrypted_req.plaintext, 0)[0] if len(decrypted_req.plaintext) >= 4 else None
                if decrypted_req.source_hash == self._local_hash and decrypted_req.destination_hash == remote_hash:
                    last_observation = f"echoed-own-req:{request_tag}"
                    self.logger.info(
                        "ignored echoed own request expected_tag=%s echoed_tag=%s src=%s dst=%s",
                        expected_tag,
                        request_tag,
                        decrypted_req.source_hash.hex().upper(),
                        decrypted_req.destination_hash.hex().upper(),
                    )
                    continue
                last_observation = f"unexpected-req:{request_tag}"
                self.logger.info(
                    "ignored foreign req while waiting expected_tag=%s actual_tag=%s src=%s dst=%s",
                    expected_tag,
                    request_tag,
                    decrypted_req.source_hash.hex().upper(),
                    decrypted_req.destination_hash.hex().upper(),
                )
                continue
            if summary.payload_type is not PayloadType.RESPONSE:
                last_observation = f"ignored-payload-type:{summary.payload_type.name}"
                self.logger.info(
                    "ignored frame while waiting for tag=%s payload_type=%s",
                    expected_tag,
                    summary.payload_type.name,
                )
                continue
            try:
                decrypted = parse_encrypted_datagram(summary, shared_secret=shared_secret)
            except Exception as exc:
                last_observation = f"response-decrypt-failed:{exc}"
                self.logger.info("ignored response frame tag=%s reason=%s", expected_tag, exc)
                continue
            if not self._is_remote_to_local_datagram(
                source_hash=decrypted.source_hash,
                destination_hash=decrypted.destination_hash,
                remote_hash=remote_hash,
            ):
                last_observation = (
                    "foreign-response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored RESPONSE from foreign hashes expected_tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                continue
            if self._is_login_response_payload(decrypted.plaintext):
                last_observation = (
                    "late-login-response"
                    f":src={decrypted.source_hash.hex().upper()}"
                    f":dst={decrypted.destination_hash.hex().upper()}"
                )
                self.logger.info(
                    "ignored late login RESPONSE while waiting for tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                continue
            if len(decrypted.plaintext) >= 4 and struct.unpack_from("<I", decrypted.plaintext, 0)[0] == expected_tag:
                self.logger.info(
                    "accepted RESPONSE tag=%s src=%s dst=%s",
                    expected_tag,
                    decrypted.source_hash.hex().upper(),
                    decrypted.destination_hash.hex().upper(),
                )
                return decrypted.plaintext, current_path_len, current_path_bytes
            actual_tag = struct.unpack_from("<I", decrypted.plaintext, 0)[0] if len(decrypted.plaintext) >= 4 else None
            last_observation = (
                f"response-tag-mismatch={actual_tag}"
                f":src={decrypted.source_hash.hex().upper()}"
                f":dst={decrypted.destination_hash.hex().upper()}"
            )
            self.logger.info(
                "ignored RESPONSE expected_tag=%s actual_tag=%s src=%s dst=%s",
                expected_tag,
                actual_tag,
                decrypted.source_hash.hex().upper(),
                decrypted.destination_hash.hex().upper(),
            )

    def _is_remote_to_local_datagram(self, *, source_hash: bytes, destination_hash: bytes, remote_hash: bytes) -> bool:
        return source_hash == remote_hash and destination_hash == self._local_hash

    def _is_usable_stored_path(self, path_row: dict[str, object]) -> bool:
        path_len = int(cast(int | str, path_row.get("out_path_len", path_row.get("path_len", 0))) or 0)
        path_hex = str(cast(str | None, path_row.get("out_path_hex", path_row.get("path_hex", ""))) or "").strip()
        return path_len > 0 and path_hex != ""

    def _is_login_response_payload(self, payload: bytes) -> bool:
        if len(payload) not in {12, 13}:
            return False
        try:
            login = parse_login_response(payload)
        except Exception:
            return False
        return login.response_code == RESP_SERVER_LOGIN_OK

    def _save_repeater_path_update(
        self,
        *,
        repeater_id: int,
        encoded_path_len: int,
        path_bytes: bytes,
        source: str,
    ) -> tuple[int, bytes]:
        if not encoded_path_len:
            return 0, b""
        self.database.save_repeater_path(
            repeater_id=repeater_id,
            encoded_path_len=encoded_path_len,
            path_hex=path_bytes.hex().upper(),
            source=source,
        )
        return encoded_path_len, path_bytes

    def _record_rx(self, endpoint_name: str, probe_run_id: int, remote_pubkey: bytes, received: ReceivedPacket) -> None:
        self.database.insert_raw_packet(
            probe_run_id=probe_run_id,
            endpoint_name=endpoint_name,
            observed_at=received.observed_at,
            direction="rx",
            transport="rs232bridge",
            rs232_frame_hex=received.frame_hex,
            mesh_packet_hex=received.packet_hex,
            payload_type=int(received.summary.payload_type),
            route_type=int(received.summary.route_type),
            remote_pubkey_hex=remote_pubkey.hex().upper(),
        )
        self.logger.info(
            "decoded rx frame endpoint=%s repeater=%s %s frame=%s packet=%s",
            endpoint_name,
            remote_pubkey.hex().upper()[:12],
            describe_packet_summary(received.summary),
            received.frame_hex,
            received.packet_hex,
        )

