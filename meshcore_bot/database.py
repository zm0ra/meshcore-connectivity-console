from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any

from .config import AppConfig, ChannelConfig, EndpointConfig, ManagementTargetConfig


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


class BotDatabase:
    SCHEMA_VERSION = 1

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS schema_info (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS config_channels (
                    name TEXT PRIMARY KEY,
                    psk TEXT,
                    listen INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS config_endpoints (
                    name TEXT PRIMARY KEY,
                    raw_host TEXT NOT NULL,
                    raw_port INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    console_host TEXT,
                    console_port INTEGER,
                    console_mirror_host TEXT,
                    console_mirror_port INTEGER,
                    latitude REAL,
                    longitude REAL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS management_targets (
                    name TEXT PRIMARY KEY,
                    endpoint_name TEXT NOT NULL,
                    target_hash_prefix TEXT,
                    target_identity_hex TEXT,
                    guest_password TEXT,
                    admin_password TEXT,
                    prefer_role TEXT NOT NULL DEFAULT 'guest',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    notes TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    identity_hex TEXT PRIMARY KEY,
                    hash_prefix_hex TEXT NOT NULL,
                    name TEXT,
                    role TEXT,
                    latitude REAL,
                    longitude REAL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_seen_endpoint TEXT,
                    last_source TEXT NOT NULL DEFAULT 'radio'
                );

                CREATE TABLE IF NOT EXISTS advert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    identity_hex TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    heard_at TEXT NOT NULL,
                    advertised_at INTEGER,
                    name TEXT,
                    role TEXT,
                    latitude REAL,
                    longitude REAL,
                    raw_payload_hex TEXT,
                    FOREIGN KEY(identity_hex) REFERENCES nodes(identity_hex)
                );

                CREATE INDEX IF NOT EXISTS idx_advert_history_identity ON advert_history(identity_hex);
                CREATE INDEX IF NOT EXISTS idx_advert_history_heard_at ON advert_history(heard_at DESC);

                CREATE TABLE IF NOT EXISTS radio_packets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint_name TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    frame_valid INTEGER NOT NULL DEFAULT 1,
                    route_name TEXT,
                    packet_type TEXT,
                    path_len INTEGER,
                    transport_codes_json TEXT,
                    raw_frame_hex TEXT,
                    payload_hex TEXT,
                    source_kind TEXT NOT NULL DEFAULT 'raw',
                    snr REAL,
                    rssi INTEGER,
                    notes TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_radio_packets_observed_at ON radio_packets(observed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_radio_packets_endpoint ON radio_packets(endpoint_name, observed_at DESC);

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint_name TEXT NOT NULL,
                    channel_name TEXT,
                    sender TEXT,
                    sender_identity_hex TEXT,
                    content TEXT,
                    packet_type TEXT,
                    route_name TEXT,
                    path_len INTEGER,
                    received_at TEXT NOT NULL,
                    snr REAL,
                    rssi INTEGER,
                    distance_km REAL,
                    raw_payload_hex TEXT,
                    source_kind TEXT NOT NULL DEFAULT 'radio'
                );

                CREATE INDEX IF NOT EXISTS idx_messages_received_at ON messages(received_at DESC);
                CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_name, received_at DESC);

                CREATE TABLE IF NOT EXISTS command_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint_name TEXT,
                    channel_name TEXT,
                    sender TEXT,
                    command_name TEXT NOT NULL,
                    command_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    responded_at TEXT,
                    response_text TEXT,
                    related_message_id INTEGER,
                    related_packet_id INTEGER,
                    notes TEXT,
                    FOREIGN KEY(related_message_id) REFERENCES messages(id),
                    FOREIGN KEY(related_packet_id) REFERENCES radio_packets(id)
                );

                CREATE INDEX IF NOT EXISTS idx_command_events_received_at ON command_events(received_at DESC);
                CREATE INDEX IF NOT EXISTS idx_command_events_name ON command_events(command_name, received_at DESC);

                CREATE TABLE IF NOT EXISTS neighbor_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    requester_role TEXT,
                    collected_at TEXT NOT NULL,
                    success INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS neighbor_edges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    neighbor_hash_prefix TEXT,
                    neighbor_identity_hex TEXT,
                    snr REAL,
                    rssi INTEGER,
                    last_heard_seconds INTEGER,
                    label TEXT,
                    FOREIGN KEY(snapshot_id) REFERENCES neighbor_snapshots(id)
                );

                CREATE TABLE IF NOT EXISTS owner_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    requester_role TEXT,
                    collected_at TEXT NOT NULL,
                    firmware_version TEXT,
                    node_name TEXT,
                    owner_info TEXT,
                    raw_text TEXT
                );

                CREATE TABLE IF NOT EXISTS acl_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    requester_role TEXT,
                    collected_at TEXT NOT NULL,
                    success INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT
                );

                CREATE TABLE IF NOT EXISTS acl_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    pubkey_prefix_hex TEXT NOT NULL,
                    permissions INTEGER NOT NULL,
                    FOREIGN KEY(snapshot_id) REFERENCES acl_snapshots(id)
                );
                """
            )
            now_iso = _utc_now_iso()
            connection.execute(
                """
                INSERT INTO schema_info (key, value, updated_at)
                VALUES ('schema_version', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (str(self.SCHEMA_VERSION), now_iso),
            )

    def bootstrap_from_config(self, config: AppConfig) -> None:
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM config_channels")
            for channel in config.channels:
                self._insert_channel(connection, channel, now_iso)

            connection.execute("DELETE FROM config_endpoints")
            for endpoint in config.endpoints:
                self._insert_endpoint(connection, endpoint, now_iso)

            connection.execute("DELETE FROM management_targets")
            for target in config.management_targets:
                self._insert_management_target(connection, target, now_iso)

            self._set_json_setting(
                connection,
                "runtime.bot",
                {
                    "name": config.bot.name,
                    "reply_prefix": config.bot.reply_prefix,
                    "command_prefix": config.bot.command_prefix,
                    "message_history_size": config.bot.message_history_size,
                },
                now_iso,
            )
            self._set_json_setting(
                connection,
                "runtime.web",
                {
                    "host": config.web.host,
                    "port": config.web.port,
                },
                now_iso,
            )

    def list_settings(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT key, value_json FROM app_settings ORDER BY key"
            ).fetchall()
        return {str(row["key"]): json.loads(row["value_json"]) for row in rows}

    def record_radio_packet(
        self,
        *,
        endpoint_name: str,
        direction: str,
        observed_at: datetime | None = None,
        frame_valid: bool = True,
        route_name: str | None = None,
        packet_type: str | None = None,
        path_len: int | None = None,
        transport_codes: list[int] | tuple[int, ...] | None = None,
        raw_frame_hex: str | None = None,
        payload_hex: str | None = None,
        source_kind: str = "raw",
        snr: float | None = None,
        rssi: int | None = None,
        notes: str | None = None,
    ) -> int:
        observed_at_iso = (observed_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        transport_codes_json = json.dumps(list(transport_codes)) if transport_codes is not None else None
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO radio_packets (
                    endpoint_name, direction, observed_at, frame_valid, route_name, packet_type,
                    path_len, transport_codes_json, raw_frame_hex, payload_hex, source_kind, snr, rssi, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    endpoint_name,
                    direction,
                    observed_at_iso,
                    1 if frame_valid else 0,
                    route_name,
                    packet_type,
                    path_len,
                    transport_codes_json,
                    raw_frame_hex,
                    payload_hex,
                    source_kind,
                    snr,
                    rssi,
                    notes,
                ),
            )
            return int(cursor.lastrowid)

    def record_message(
        self,
        *,
        endpoint_name: str,
        channel_name: str | None,
        sender: str | None,
        sender_identity_hex: str | None,
        content: str | None,
        packet_type: str | None,
        route_name: str | None,
        path_len: int | None,
        received_at: datetime | None = None,
        snr: float | None = None,
        rssi: int | None = None,
        distance_km: float | None = None,
        raw_payload_hex: str | None = None,
        source_kind: str = "radio",
    ) -> int:
        received_at_iso = (received_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO messages (
                    endpoint_name, channel_name, sender, sender_identity_hex, content,
                    packet_type, route_name, path_len, received_at, snr, rssi,
                    distance_km, raw_payload_hex, source_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    endpoint_name,
                    channel_name,
                    sender,
                    sender_identity_hex,
                    content,
                    packet_type,
                    route_name,
                    path_len,
                    received_at_iso,
                    snr,
                    rssi,
                    distance_km,
                    raw_payload_hex,
                    source_kind,
                ),
            )
            return int(cursor.lastrowid)

    def record_command_event(
        self,
        *,
        command_name: str,
        command_text: str,
        status: str,
        endpoint_name: str | None = None,
        channel_name: str | None = None,
        sender: str | None = None,
        received_at: datetime | None = None,
        responded_at: datetime | None = None,
        response_text: str | None = None,
        related_message_id: int | None = None,
        related_packet_id: int | None = None,
        notes: str | None = None,
    ) -> int:
        received_at_iso = (received_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        responded_at_iso = responded_at.astimezone(UTC).isoformat() if responded_at else None
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO command_events (
                    endpoint_name, channel_name, sender, command_name, command_text, status,
                    received_at, responded_at, response_text, related_message_id, related_packet_id, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    endpoint_name,
                    channel_name,
                    sender,
                    command_name,
                    command_text,
                    status,
                    received_at_iso,
                    responded_at_iso,
                    response_text,
                    related_message_id,
                    related_packet_id,
                    notes,
                ),
            )
            return int(cursor.lastrowid)

    def snapshot_overview(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            return {
                "database_path": str(self.database_path),
                "schema_version": self._scalar(connection, "SELECT value FROM schema_info WHERE key = 'schema_version'"),
                "config_channel_count": self._scalar(connection, "SELECT COUNT(*) FROM config_channels"),
                "config_endpoint_count": self._scalar(connection, "SELECT COUNT(*) FROM config_endpoints"),
                "management_target_count": self._scalar(connection, "SELECT COUNT(*) FROM management_targets"),
                "node_count": self._scalar(connection, "SELECT COUNT(*) FROM nodes"),
                "advert_count": self._scalar(connection, "SELECT COUNT(*) FROM advert_history"),
                "radio_packet_count": self._scalar(connection, "SELECT COUNT(*) FROM radio_packets"),
                "message_count": self._scalar(connection, "SELECT COUNT(*) FROM messages"),
                "command_event_count": self._scalar(connection, "SELECT COUNT(*) FROM command_events"),
                "neighbor_snapshot_count": self._scalar(connection, "SELECT COUNT(*) FROM neighbor_snapshots"),
                "owner_snapshot_count": self._scalar(connection, "SELECT COUNT(*) FROM owner_snapshots"),
                "acl_snapshot_count": self._scalar(connection, "SELECT COUNT(*) FROM acl_snapshots"),
            }

    def _scalar(self, connection: sqlite3.Connection, query: str) -> Any:
        row = connection.execute(query).fetchone()
        return row[0] if row is not None else None

    def _set_json_setting(
        self,
        connection: sqlite3.Connection,
        key: str,
        value: Any,
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO app_settings (key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
            """,
            (key, json.dumps(value, ensure_ascii=True, sort_keys=True), updated_at),
        )

    def _insert_channel(self, connection: sqlite3.Connection, channel: ChannelConfig, updated_at: str) -> None:
        connection.execute(
            """
            INSERT INTO config_channels (name, psk, listen, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (channel.name, channel.psk, 1 if channel.listen else 0, updated_at),
        )

    def _insert_endpoint(self, connection: sqlite3.Connection, endpoint: EndpointConfig, updated_at: str) -> None:
        connection.execute(
            """
            INSERT INTO config_endpoints (
                name, raw_host, raw_port, enabled, console_host, console_port,
                console_mirror_host, console_mirror_port, latitude, longitude, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                endpoint.name,
                endpoint.raw_host,
                endpoint.raw_port,
                1 if endpoint.enabled else 0,
                endpoint.console_host,
                endpoint.console_port,
                endpoint.console_mirror_host,
                endpoint.console_mirror_port,
                endpoint.latitude,
                endpoint.longitude,
                updated_at,
            ),
        )

    def _insert_management_target(
        self,
        connection: sqlite3.Connection,
        target: ManagementTargetConfig,
        updated_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO management_targets (
                name, endpoint_name, target_hash_prefix, target_identity_hex,
                guest_password, admin_password, prefer_role, enabled, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target.name,
                target.endpoint_name,
                target.target_hash_prefix,
                target.target_identity_hex,
                target.guest_password,
                target.admin_password,
                target.prefer_role,
                1 if target.enabled else 0,
                target.notes,
                updated_at,
            ),
        )