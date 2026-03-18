from __future__ import annotations

from datetime import UTC, datetime
from datetime import timedelta
from pathlib import Path
import sqlite3
import time
from typing import Callable, TypeVar


T = TypeVar("T")


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def is_recent_iso_timestamp(value: str | None, max_age_secs: float, *, now: datetime | None = None) -> bool:
    if not value or max_age_secs <= 0:
        return False
    if now is None:
        now = datetime.now(tz=UTC)
    try:
        observed = datetime.fromisoformat(value)
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    age_secs = (now - observed).total_seconds()
    return 0 <= age_secs <= max_age_secs


class BotDatabase:
    SCHEMA_VERSION = 4
    CONNECT_TIMEOUT_SECS = 30.0
    BUSY_TIMEOUT_MS = 30_000
    WRITE_RETRY_ATTEMPTS = 5
    WRITE_RETRY_DELAY_SECS = 0.25

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=self.CONNECT_TIMEOUT_SECS)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def initialize(self) -> None:
        now_iso = utc_now_iso()
        with self.connect() as connection:
            connection.executescript(
                """
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS schema_info (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS repeaters (
                    id INTEGER PRIMARY KEY,
                    pubkey BLOB NOT NULL UNIQUE,
                    pubkey_hex TEXT NOT NULL UNIQUE,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_name_from_advert TEXT,
                    last_lat REAL,
                    last_lon REAL,
                    last_advert_timestamp_remote INTEGER,
                    last_firmware_version TEXT,
                    last_owner_info TEXT,
                    last_guest_permissions INTEGER,
                    last_firmware_capability_level INTEGER,
                    last_login_server_time INTEGER,
                    learned_login_role TEXT,
                    learned_login_password TEXT,
                    learned_login_success_count INTEGER NOT NULL DEFAULT 0,
                    learned_login_updated_at TEXT,
                    preferred_endpoint_name TEXT,
                    preferred_endpoint_updated_at TEXT,
                    last_probe_status TEXT,
                    last_probe_at TEXT
                );

                CREATE TABLE IF NOT EXISTS repeater_adverts (
                    id INTEGER PRIMARY KEY,
                    repeater_id INTEGER NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    advert_timestamp_remote INTEGER,
                    advert_name TEXT,
                    advert_lat REAL,
                    advert_lon REAL,
                    path_len INTEGER,
                    path_hex TEXT,
                    raw_packet_hex TEXT NOT NULL,
                    FOREIGN KEY (repeater_id) REFERENCES repeaters(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_paths (
                    id INTEGER PRIMARY KEY,
                    repeater_id INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    out_path_len INTEGER NOT NULL,
                    out_path_hex TEXT NOT NULL,
                    source TEXT NOT NULL,
                    FOREIGN KEY (repeater_id) REFERENCES repeaters(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_probe_runs (
                    id INTEGER PRIMARY KEY,
                    repeater_id INTEGER NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    result TEXT NOT NULL,
                    guest_login_ok INTEGER NOT NULL DEFAULT 0,
                    guest_permissions INTEGER,
                    firmware_capability_level INTEGER,
                    login_server_time INTEGER,
                    error_message TEXT,
                    FOREIGN KEY (repeater_id) REFERENCES repeaters(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_owner_snapshots (
                    id INTEGER PRIMARY KEY,
                    probe_run_id INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    firmware_version TEXT,
                    node_name TEXT,
                    owner_info TEXT,
                    FOREIGN KEY (probe_run_id) REFERENCES repeater_probe_runs(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_status_snapshots (
                    id INTEGER PRIMARY KEY,
                    probe_run_id INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    batt_milli_volts INTEGER,
                    curr_tx_queue_len INTEGER,
                    noise_floor INTEGER,
                    last_rssi INTEGER,
                    n_packets_recv INTEGER,
                    n_packets_sent INTEGER,
                    total_air_time_secs INTEGER,
                    total_up_time_secs INTEGER,
                    n_sent_flood INTEGER,
                    n_sent_direct INTEGER,
                    n_recv_flood INTEGER,
                    n_recv_direct INTEGER,
                    err_events INTEGER,
                    last_snr REAL,
                    n_direct_dups INTEGER,
                    n_flood_dups INTEGER,
                    total_rx_air_time_secs INTEGER,
                    n_recv_errors INTEGER,
                    FOREIGN KEY (probe_run_id) REFERENCES repeater_probe_runs(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_telemetry_snapshots (
                    id INTEGER PRIMARY KEY,
                    probe_run_id INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    cayenne_lpp_hex TEXT NOT NULL,
                    decoded_json TEXT,
                    FOREIGN KEY (probe_run_id) REFERENCES repeater_probe_runs(id)
                );

                CREATE TABLE IF NOT EXISTS repeater_neighbour_snapshots (
                    id INTEGER PRIMARY KEY,
                    probe_run_id INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    page_offset INTEGER NOT NULL,
                    total_neighbours_count INTEGER NOT NULL,
                    results_count INTEGER NOT NULL,
                    neighbour_pubkey_prefix_hex TEXT NOT NULL,
                    heard_seconds_ago INTEGER NOT NULL,
                    snr REAL NOT NULL,
                    FOREIGN KEY (probe_run_id) REFERENCES repeater_probe_runs(id)
                );

                CREATE TABLE IF NOT EXISTS raw_mesh_packets (
                    id INTEGER PRIMARY KEY,
                    probe_run_id INTEGER,
                    endpoint_name TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    transport TEXT NOT NULL,
                    rs232_frame_hex TEXT,
                    mesh_packet_hex TEXT NOT NULL,
                    payload_type INTEGER,
                    route_type INTEGER,
                    remote_pubkey_hex TEXT,
                    request_tag INTEGER,
                    notes TEXT,
                    FOREIGN KEY (probe_run_id) REFERENCES repeater_probe_runs(id)
                );

                CREATE TABLE IF NOT EXISTS probe_jobs (
                    id INTEGER PRIMARY KEY,
                    repeater_id INTEGER NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    FOREIGN KEY (repeater_id) REFERENCES repeaters(id)
                );

                CREATE INDEX IF NOT EXISTS idx_probe_jobs_status_scheduled_at
                ON probe_jobs(status, scheduled_at, id);
                """
            )
            self._ensure_column(connection, "repeater_adverts", "endpoint_name", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "repeater_probe_runs", "endpoint_name", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "raw_mesh_packets", "endpoint_name", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, "repeaters", "learned_login_role", "TEXT")
            self._ensure_column(connection, "repeaters", "learned_login_password", "TEXT")
            self._ensure_column(connection, "repeaters", "learned_login_success_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(connection, "repeaters", "learned_login_updated_at", "TEXT")
            self._ensure_column(connection, "repeaters", "preferred_endpoint_name", "TEXT")
            self._ensure_column(connection, "repeaters", "preferred_endpoint_updated_at", "TEXT")
            connection.execute(
                """
                INSERT INTO schema_info (key, value, updated_at)
                VALUES ('schema_version', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (str(self.SCHEMA_VERSION), now_iso),
            )

    def snapshot_overview(self) -> dict[str, int | str | None]:
        with self.connect() as connection:
            return {
                "database_path": str(self.database_path),
                "schema_version": self._scalar(connection, "SELECT value FROM schema_info WHERE key = 'schema_version'"),
                "repeater_count": self._scalar(connection, "SELECT COUNT(*) FROM repeaters"),
                "advert_count": self._scalar(connection, "SELECT COUNT(*) FROM repeater_adverts"),
                "probe_run_count": self._scalar(connection, "SELECT COUNT(*) FROM repeater_probe_runs"),
                "probe_job_count": self._scalar(connection, "SELECT COUNT(*) FROM probe_jobs"),
                "raw_packet_count": self._scalar(connection, "SELECT COUNT(*) FROM raw_mesh_packets"),
            }

    def upsert_repeater_from_advert(
        self,
        *,
        endpoint_name: str,
        observed_at: str,
        public_key: bytes,
        advert_name: str | None,
        advert_lat: float | None,
        advert_lon: float | None,
        advert_timestamp_remote: int,
        path_len: int,
        path_hex: str,
        raw_packet_hex: str,
    ) -> int:
        pubkey_hex = public_key.hex().upper()
        def operation(connection: sqlite3.Connection) -> int:
            row = connection.execute(
                "SELECT id FROM repeaters WHERE pubkey_hex = ?",
                (pubkey_hex,),
            ).fetchone()
            if row is None:
                cursor = connection.execute(
                    """
                    INSERT INTO repeaters (
                        pubkey, pubkey_hex, first_seen_at, last_seen_at, last_name_from_advert,
                        last_lat, last_lon, last_advert_timestamp_remote, last_probe_status, last_probe_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                    """,
                    (
                        public_key,
                        pubkey_hex,
                        observed_at,
                        observed_at,
                        advert_name,
                        advert_lat,
                        advert_lon,
                        advert_timestamp_remote,
                    ),
                )
                lastrowid = cursor.lastrowid
                assert lastrowid is not None
                repeater_id = int(lastrowid)
            else:
                repeater_id = int(row["id"])
                connection.execute(
                    """
                    UPDATE repeaters
                    SET last_seen_at = ?,
                        last_name_from_advert = ?,
                        last_lat = ?,
                        last_lon = ?,
                        last_advert_timestamp_remote = ?
                    WHERE id = ?
                    """,
                    (
                        observed_at,
                        advert_name,
                        advert_lat,
                        advert_lon,
                        advert_timestamp_remote,
                        repeater_id,
                    ),
                )

            connection.execute(
                """
                INSERT INTO repeater_adverts (
                    repeater_id, endpoint_name, observed_at, advert_timestamp_remote,
                    advert_name, advert_lat, advert_lon, path_len, path_hex, raw_packet_hex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    repeater_id,
                    endpoint_name,
                    observed_at,
                    advert_timestamp_remote,
                    advert_name,
                    advert_lat,
                    advert_lon,
                    path_len,
                    path_hex,
                    raw_packet_hex,
                ),
            )
            return repeater_id

        return self._run_with_retry(operation)

    def insert_raw_packet(
        self,
        *,
        endpoint_name: str,
        observed_at: str,
        direction: str,
        transport: str,
        mesh_packet_hex: str,
        payload_type: int | None,
        route_type: int | None,
        rs232_frame_hex: str | None = None,
        probe_run_id: int | None = None,
        remote_pubkey_hex: str | None = None,
        request_tag: int | None = None,
        notes: str | None = None,
    ) -> int:
        def operation(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                """
                INSERT INTO raw_mesh_packets (
                    probe_run_id, endpoint_name, observed_at, direction, transport,
                    rs232_frame_hex, mesh_packet_hex, payload_type, route_type,
                    remote_pubkey_hex, request_tag, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    probe_run_id,
                    endpoint_name,
                    observed_at,
                    direction,
                    transport,
                    rs232_frame_hex,
                    mesh_packet_hex,
                    payload_type,
                    route_type,
                    remote_pubkey_hex,
                    request_tag,
                    notes,
                ),
            )
            lastrowid = cursor.lastrowid
            assert lastrowid is not None
            return int(lastrowid)

        return self._run_with_retry(operation)

    def enqueue_probe_job(
        self,
        *,
        repeater_id: int,
        endpoint_name: str,
        reason: str,
        success_cooldown_secs: float = 0.0,
        failure_cooldown_secs: float = 0.0,
        scheduled_at: str | None = None,
        max_recent_jobs: int | None = None,
        recent_window_secs: float = 86400.0,
    ) -> int | None:
        scheduled_time = scheduled_at or utc_now_iso()
        def operation(connection: sqlite3.Connection) -> int | None:
            existing = connection.execute(
                """
                SELECT id FROM probe_jobs
                WHERE repeater_id = ? AND endpoint_name = ? AND status IN ('pending', 'running')
                ORDER BY id DESC LIMIT 1
                """,
                (repeater_id, endpoint_name),
            ).fetchone()
            if existing is not None:
                return None
            if max_recent_jobs is not None and max_recent_jobs > 0 and recent_window_secs > 0:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                if scheduled_dt.tzinfo is None:
                    scheduled_dt = scheduled_dt.replace(tzinfo=UTC)
                recent_cutoff = (scheduled_dt - timedelta(seconds=recent_window_secs)).isoformat()
                recent_job_count = int(
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM probe_jobs
                        WHERE repeater_id = ?
                          AND endpoint_name = ?
                          AND scheduled_at >= ?
                        """,
                        (repeater_id, endpoint_name, recent_cutoff),
                    ).fetchone()[0]
                )
                if recent_job_count >= max_recent_jobs:
                    return None
            if success_cooldown_secs > 0 or failure_cooldown_secs > 0:
                latest = connection.execute(
                    """
                    SELECT status, finished_at, started_at, scheduled_at
                    FROM probe_jobs
                    WHERE repeater_id = ? AND endpoint_name = ? AND reason = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id, endpoint_name, reason),
                ).fetchone()
                if latest is not None:
                    cooldown_secs = 0.0
                    if latest["status"] == "completed":
                        cooldown_secs = success_cooldown_secs
                    elif latest["status"] in {"failed", "interrupted"}:
                        cooldown_secs = failure_cooldown_secs
                    latest_activity_at = latest["finished_at"] or latest["started_at"] or latest["scheduled_at"]
                    if cooldown_secs > 0 and is_recent_iso_timestamp(latest_activity_at, cooldown_secs):
                        return None
            cursor = connection.execute(
                """
                INSERT INTO probe_jobs (
                    repeater_id, endpoint_name, reason, status, scheduled_at
                ) VALUES (?, ?, ?, 'pending', ?)
                """,
                (repeater_id, endpoint_name, reason, scheduled_time),
            )
            lastrowid = cursor.lastrowid
            assert lastrowid is not None
            return int(lastrowid)

        return self._run_with_retry(operation)

    def preferred_repeater_login(self, *, repeater_id: int) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT learned_login_role, learned_login_password, learned_login_success_count, learned_login_updated_at
                FROM repeaters
                WHERE id = ?
                  AND learned_login_role IS NOT NULL
                  AND learned_login_password IS NOT NULL
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def remember_repeater_login(self, *, repeater_id: int, login_role: str, login_password: str) -> None:
        remembered_at = utc_now_iso()

        def operation(connection: sqlite3.Connection) -> None:
            existing = connection.execute(
                """
                SELECT learned_login_role, learned_login_password, learned_login_success_count
                FROM repeaters
                WHERE id = ?
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            success_count = 1
            if existing is not None:
                same_login = (
                    str(existing["learned_login_role"] or "") == login_role
                    and str(existing["learned_login_password"] or "") == login_password
                )
                if same_login:
                    success_count = max(1, int(existing["learned_login_success_count"] or 0) + 1)
            connection.execute(
                """
                UPDATE repeaters
                SET learned_login_role = ?,
                    learned_login_password = ?,
                    learned_login_success_count = ?,
                    learned_login_updated_at = ?
                WHERE id = ?
                """,
                (login_role, login_password, success_count, remembered_at, repeater_id),
            )

        self._run_with_retry(operation)

    def preferred_repeater_endpoint(self, *, repeater_id: int) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT preferred_endpoint_name, preferred_endpoint_updated_at
                FROM repeaters
                WHERE id = ?
                  AND preferred_endpoint_name IS NOT NULL
                  AND preferred_endpoint_name != ''
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def set_repeater_preferred_endpoint(self, *, repeater_id: int, endpoint_name: str) -> None:
        preferred_at = utc_now_iso()

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                UPDATE repeaters
                SET preferred_endpoint_name = ?,
                    preferred_endpoint_updated_at = ?
                WHERE id = ?
                """,
                (endpoint_name, preferred_at, repeater_id),
            )

        self._run_with_retry(operation)

    def reset_repeater_login_if_stable(self, *, repeater_id: int, min_success_count: int) -> bool:
        def operation(connection: sqlite3.Connection) -> bool:
            row = connection.execute(
                """
                SELECT learned_login_success_count, learned_login_role, learned_login_password
                FROM repeaters
                WHERE id = ?
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            if row is None:
                return False
            success_count = int(row["learned_login_success_count"] or 0)
            has_login = bool(row["learned_login_role"]) and row["learned_login_password"] is not None
            if not has_login or success_count < min_success_count:
                return False
            connection.execute(
                """
                UPDATE repeaters
                SET learned_login_role = NULL,
                    learned_login_password = NULL,
                    learned_login_success_count = 0,
                    learned_login_updated_at = ?
                WHERE id = ?
                """,
                (utc_now_iso(), repeater_id),
            )
            return True

        return self._run_with_retry(operation)

    def schedule_stale_repeater_probe_jobs(
        self,
        *,
        endpoint_names: list[str],
        stale_after_secs: float,
        seen_within_secs: float,
        reason: str,
        success_cooldown_secs: float,
        failure_cooldown_secs: float,
        max_recent_jobs: int | None = None,
        now: datetime | None = None,
    ) -> int:
        if stale_after_secs <= 0 or seen_within_secs <= 0 or not endpoint_names:
            return 0
        if now is None:
            now = datetime.now(tz=UTC)
        recent_cutoff_iso = (now - timedelta(seconds=seen_within_secs)).isoformat()
        placeholders = ",".join("?" for _ in endpoint_names)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                WITH latest_advert AS (
                    SELECT ra.repeater_id, ra.endpoint_name
                    FROM repeater_adverts ra
                    JOIN (
                        SELECT repeater_id, MAX(id) AS max_id
                        FROM repeater_adverts
                        GROUP BY repeater_id
                    ) latest ON latest.max_id = ra.id
                )
                SELECT r.id,
                      r.preferred_endpoint_name,
                       la.endpoint_name,
                       r.last_seen_at,
                       (
                           SELECT MAX(ns.observed_at)
                           FROM repeater_probe_runs pr
                           JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                           WHERE pr.repeater_id = r.id
                       ) AS last_data_at
                FROM repeaters r
                JOIN latest_advert la ON la.repeater_id = r.id
                WHERE r.last_seen_at >= ?
                  AND la.endpoint_name IN ({placeholders})
                ORDER BY r.last_seen_at DESC, r.id DESC
                """,
                (recent_cutoff_iso, *endpoint_names),
            ).fetchall()

        enqueued = 0
        for row in rows:
            last_data_at = row["last_data_at"]
            if last_data_at and is_recent_iso_timestamp(str(last_data_at), stale_after_secs, now=now):
                continue
            endpoint_name = str(row["preferred_endpoint_name"] or row["endpoint_name"] or "").strip()
            if not endpoint_name or endpoint_name not in endpoint_names:
                continue
            job_id = self.enqueue_probe_job(
                repeater_id=int(row["id"]),
                endpoint_name=endpoint_name,
                reason=reason,
                success_cooldown_secs=success_cooldown_secs,
                failure_cooldown_secs=failure_cooldown_secs,
                max_recent_jobs=max_recent_jobs,
            )
            if job_id is not None:
                enqueued += 1
        return enqueued

    def schedule_recent_failed_repeater_probe_jobs(
        self,
        *,
        endpoint_names: list[str],
        seen_within_secs: float,
        reason: str,
        success_cooldown_secs: float,
        failure_cooldown_secs: float,
        max_recent_jobs: int | None = None,
        now: datetime | None = None,
    ) -> int:
        if seen_within_secs <= 0 or not endpoint_names:
            return 0
        if now is None:
            now = datetime.now(tz=UTC)
        recent_cutoff_iso = (now - timedelta(seconds=seen_within_secs)).isoformat()
        placeholders = ",".join("?" for _ in endpoint_names)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                WITH latest_advert AS (
                    SELECT ra.repeater_id, ra.endpoint_name
                    FROM repeater_adverts ra
                    JOIN (
                        SELECT repeater_id, MAX(id) AS max_id
                        FROM repeater_adverts
                        GROUP BY repeater_id
                    ) latest ON latest.max_id = ra.id
                )
                SELECT r.id,
                      r.preferred_endpoint_name,
                      la.endpoint_name,
                       r.last_seen_at,
                       r.last_probe_status,
                       r.last_probe_at
                FROM repeaters r
                JOIN latest_advert la ON la.repeater_id = r.id
                WHERE r.last_seen_at >= ?
                  AND la.endpoint_name IN ({placeholders})
                  AND r.last_probe_status = 'failed'
                ORDER BY r.last_seen_at DESC, r.id DESC
                """,
                (recent_cutoff_iso, *endpoint_names),
            ).fetchall()

        enqueued = 0
        for row in rows:
            preferred_endpoint = str(row["preferred_endpoint_name"] or "").strip()
            latest_endpoint = str(row["endpoint_name"] or "").strip()
            candidate_names = [preferred_endpoint] if preferred_endpoint else []
            candidate_names.extend(name for name in endpoint_names if name != preferred_endpoint)
            if latest_endpoint and latest_endpoint not in candidate_names:
                candidate_names.append(latest_endpoint)
            seen: set[str] = set()
            for endpoint_name in candidate_names:
                if not endpoint_name or endpoint_name in seen or endpoint_name not in endpoint_names:
                    continue
                seen.add(endpoint_name)
                job_id = self.enqueue_probe_job(
                    repeater_id=int(row["id"]),
                    endpoint_name=endpoint_name,
                    reason=reason,
                    success_cooldown_secs=success_cooldown_secs,
                    failure_cooldown_secs=failure_cooldown_secs,
                    max_recent_jobs=max_recent_jobs,
                )
                if job_id is not None:
                    enqueued += 1
        return enqueued

    def repeater_probe_state(self, *, repeater_id: int) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT id, last_seen_at, last_name_from_advert, last_probe_status, last_probe_at
                FROM repeaters
                WHERE id = ?
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def claim_probe_job(self) -> dict[str, object] | None:
        started_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> dict[str, object] | None:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT pj.id, pj.repeater_id, pj.endpoint_name, pj.reason, pj.attempts,
                       r.pubkey, r.pubkey_hex, r.last_name_from_advert
                FROM probe_jobs pj
                JOIN repeaters r ON r.id = pj.repeater_id
                WHERE pj.status = 'pending'
                    AND pj.scheduled_at <= ?
                ORDER BY pj.scheduled_at ASC, pj.id ASC
                LIMIT 1
                  """,
                  (started_at,),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            connection.execute(
                """
                UPDATE probe_jobs
                SET status = 'running', started_at = ?, attempts = attempts + 1
                WHERE id = ?
                """,
                (started_at, row["id"]),
            )
            connection.commit()
            return dict(row)

        return self._run_with_retry(operation)

    def recover_interrupted_probe_work(self) -> dict[str, int]:
        recovered_at = utc_now_iso()

        def operation(connection: sqlite3.Connection) -> dict[str, int]:
            running_jobs = int(
                connection.execute("SELECT COUNT(*) FROM probe_jobs WHERE status = 'running'").fetchone()[0]
            )
            running_runs = int(
                connection.execute("SELECT COUNT(*) FROM repeater_probe_runs WHERE result = 'running'").fetchone()[0]
            )

            connection.execute(
                """
                UPDATE probe_jobs
                SET status = 'interrupted', finished_at = ?, last_error = COALESCE(last_error, 'worker restart recovery')
                WHERE status = 'running'
                """
                ,
                (recovered_at,),
            )
            connection.execute(
                """
                UPDATE repeater_probe_runs
                SET finished_at = ?, result = 'interrupted', error_message = COALESCE(error_message, 'worker restart recovery')
                WHERE result = 'running'
                """,
                (recovered_at,),
            )
            connection.execute(
                """
                UPDATE repeaters
                SET last_probe_status = 'interrupted', last_probe_at = ?
                WHERE last_probe_status = 'running'
                """,
                (recovered_at,),
            )
            return {"jobs_interrupted": running_jobs, "runs_interrupted": running_runs}

        return self._run_with_retry(operation)

    def finish_probe_job(self, job_id: int, *, status: str, last_error: str | None = None) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                UPDATE probe_jobs
                SET status = ?, finished_at = ?, last_error = ?
                WHERE id = ?
                """,
                (status, utc_now_iso(), last_error, job_id),
            )

        self._run_with_retry(operation)

    def delete_failed_probe_jobs_older_than(
        self,
        *,
        older_than_secs: float,
        dry_run: bool = False,
        now: datetime | None = None,
    ) -> int:
        if older_than_secs <= 0:
            return 0
        if now is None:
            now = datetime.now(tz=UTC)
        cutoff_iso = (now - timedelta(seconds=older_than_secs)).isoformat()

        def operation(connection: sqlite3.Connection) -> int:
            count = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM probe_jobs
                    WHERE status = 'failed'
                      AND COALESCE(finished_at, started_at, scheduled_at) < ?
                    """,
                    (cutoff_iso,),
                ).fetchone()[0]
            )
            if dry_run or count == 0:
                return count
            connection.execute(
                """
                DELETE FROM probe_jobs
                WHERE status = 'failed'
                  AND COALESCE(finished_at, started_at, scheduled_at) < ?
                """,
                (cutoff_iso,),
            )
            return count

        return self._run_with_retry(operation)

    def create_probe_run(self, *, repeater_id: int, endpoint_name: str) -> int:
        started_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> int:
            cursor = connection.execute(
                """
                INSERT INTO repeater_probe_runs (
                    repeater_id, endpoint_name, started_at, result
                ) VALUES (?, ?, ?, 'running')
                """,
                (repeater_id, endpoint_name, started_at),
            )
            connection.execute(
                "UPDATE repeaters SET last_probe_status = 'running', last_probe_at = ? WHERE id = ?",
                (started_at, repeater_id),
            )
            lastrowid = cursor.lastrowid
            assert lastrowid is not None
            return int(lastrowid)

        return self._run_with_retry(operation)

    def complete_probe_run(
        self,
        probe_run_id: int,
        *,
        repeater_id: int,
        result: str,
        guest_login_ok: bool,
        guest_permissions: int | None,
        firmware_capability_level: int | None,
        login_server_time: int | None,
        error_message: str | None,
    ) -> None:
        finished_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                UPDATE repeater_probe_runs
                SET finished_at = ?, result = ?, guest_login_ok = ?, guest_permissions = ?,
                    firmware_capability_level = ?, login_server_time = ?, error_message = ?
                WHERE id = ?
                """,
                (
                    finished_at,
                    result,
                    1 if guest_login_ok else 0,
                    guest_permissions,
                    firmware_capability_level,
                    login_server_time,
                    error_message,
                    probe_run_id,
                ),
            )
            connection.execute(
                """
                UPDATE repeaters
                SET last_probe_status = ?, last_probe_at = ?, last_guest_permissions = ?,
                    last_firmware_capability_level = ?, last_login_server_time = ?
                WHERE id = ?
                """,
                (
                    result,
                    finished_at,
                    guest_permissions,
                    firmware_capability_level,
                    login_server_time,
                    repeater_id,
                ),
            )

        self._run_with_retry(operation)

    def save_repeater_path(self, *, repeater_id: int, encoded_path_len: int, path_hex: str, source: str) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO repeater_paths (repeater_id, observed_at, out_path_len, out_path_hex, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (repeater_id, utc_now_iso(), encoded_path_len, path_hex, source),
            )

        self._run_with_retry(operation)

    def latest_repeater_path(self, *, repeater_id: int) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT out_path_len, out_path_hex, observed_at, source
                FROM repeater_paths
                WHERE repeater_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def latest_repeater_advert(self, *, repeater_id: int, endpoint_name: str | None = None) -> dict[str, object] | None:
        with self.connect() as connection:
            if endpoint_name is None:
                row = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND endpoint_name = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id, endpoint_name),
                ).fetchone()
            return dict(row) if row is not None else None

    def latest_repeater_zero_hop_advert(self, *, repeater_id: int, endpoint_name: str | None = None) -> dict[str, object] | None:
        with self.connect() as connection:
            if endpoint_name is None:
                row = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND COALESCE(path_len, 0) = 0 AND COALESCE(path_hex, '') = ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND endpoint_name = ?
                      AND COALESCE(path_len, 0) = 0 AND COALESCE(path_hex, '') = ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id, endpoint_name),
                ).fetchone()
            return dict(row) if row is not None else None

    def latest_repeater_advert_path(self, *, repeater_id: int, endpoint_name: str | None = None) -> dict[str, object] | None:
        with self.connect() as connection:
            if endpoint_name is None:
                row = connection.execute(
                    """
                    SELECT path_len, path_hex, observed_at, endpoint_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND path_len IS NOT NULL AND path_len > 0 AND path_hex IS NOT NULL AND path_hex != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT path_len, path_hex, observed_at, endpoint_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND endpoint_name = ?
                      AND path_len IS NOT NULL AND path_len > 0 AND path_hex IS NOT NULL AND path_hex != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (repeater_id, endpoint_name),
                ).fetchone()
            return dict(row) if row is not None else None

    def recent_repeater_advert_paths(
        self,
        *,
        repeater_id: int,
        endpoint_name: str | None = None,
        limit: int = 8,
    ) -> list[dict[str, object]]:
        with self.connect() as connection:
            if endpoint_name is None:
                rows = connection.execute(
                    """
                    SELECT path_len, path_hex, observed_at, endpoint_name
                    FROM repeater_adverts
                    WHERE repeater_id = ?
                      AND path_len IS NOT NULL AND path_len > 0 AND path_hex IS NOT NULL AND path_hex != ''
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (repeater_id, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT path_len, path_hex, observed_at, endpoint_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND endpoint_name = ?
                      AND path_len IS NOT NULL AND path_len > 0 AND path_hex IS NOT NULL AND path_hex != ''
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (repeater_id, endpoint_name, limit),
                ).fetchall()
            return [dict(row) for row in rows]

    def recent_repeater_adverts(
        self,
        *,
        repeater_id: int,
        endpoint_name: str | None = None,
        limit: int = 8,
    ) -> list[dict[str, object]]:
        with self.connect() as connection:
            if endpoint_name is None:
                rows = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (repeater_id, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT endpoint_name, observed_at, path_len, path_hex, advert_name
                    FROM repeater_adverts
                    WHERE repeater_id = ? AND endpoint_name = ?
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (repeater_id, endpoint_name, limit),
                ).fetchall()
            return [dict(row) for row in rows]

    def save_owner_snapshot(self, *, probe_run_id: int, firmware_version: str | None, node_name: str | None, owner_info: str | None) -> None:
        observed_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO repeater_owner_snapshots (probe_run_id, observed_at, firmware_version, node_name, owner_info)
                VALUES (?, ?, ?, ?, ?)
                """,
                (probe_run_id, observed_at, firmware_version, node_name, owner_info),
            )
            connection.execute(
                """
                UPDATE repeaters
                SET last_firmware_version = COALESCE(?, last_firmware_version),
                    last_owner_info = COALESCE(?, last_owner_info)
                WHERE id = (SELECT repeater_id FROM repeater_probe_runs WHERE id = ?)
                """,
                (firmware_version, owner_info, probe_run_id),
            )

        self._run_with_retry(operation)

    def save_status_snapshot(self, *, probe_run_id: int, status: dict[str, object]) -> None:
        observed_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO repeater_status_snapshots (
                    probe_run_id, observed_at, batt_milli_volts, curr_tx_queue_len, noise_floor,
                    last_rssi, n_packets_recv, n_packets_sent, total_air_time_secs, total_up_time_secs,
                    n_sent_flood, n_sent_direct, n_recv_flood, n_recv_direct, err_events, last_snr,
                    n_direct_dups, n_flood_dups, total_rx_air_time_secs, n_recv_errors
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    probe_run_id,
                    observed_at,
                    status["batt_milli_volts"],
                    status["curr_tx_queue_len"],
                    status["noise_floor"],
                    status["last_rssi"],
                    status["n_packets_recv"],
                    status["n_packets_sent"],
                    status["total_air_time_secs"],
                    status["total_up_time_secs"],
                    status["n_sent_flood"],
                    status["n_sent_direct"],
                    status["n_recv_flood"],
                    status["n_recv_direct"],
                    status["err_events"],
                    status["last_snr"],
                    status["n_direct_dups"],
                    status["n_flood_dups"],
                    status["total_rx_air_time_secs"],
                    status["n_recv_errors"],
                ),
            )

        self._run_with_retry(operation)

    def save_neighbour_snapshot_page(
        self,
        *,
        probe_run_id: int,
        page_offset: int,
        total_neighbours_count: int,
        results_count: int,
        entries: list[dict[str, object]],
    ) -> None:
        observed_at = utc_now_iso()
        def operation(connection: sqlite3.Connection) -> None:
            connection.executemany(
                """
                INSERT INTO repeater_neighbour_snapshots (
                    probe_run_id, observed_at, page_offset, total_neighbours_count,
                    results_count, neighbour_pubkey_prefix_hex, heard_seconds_ago, snr
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        probe_run_id,
                        observed_at,
                        page_offset,
                        total_neighbours_count,
                        results_count,
                        entry["neighbour_pubkey_prefix_hex"],
                        entry["heard_seconds_ago"],
                        entry["snr"],
                    )
                    for entry in entries
                ],
            )

        self._run_with_retry(operation)

    def list_repeaters(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, pubkey_hex, last_name_from_advert, last_lat, last_lon,
                       last_seen_at, last_probe_status, last_probe_at,
                       last_firmware_version, last_owner_info
                FROM repeaters
                ORDER BY last_seen_at DESC, id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def list_repeaters_for_web(self) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT r.id,
                       r.pubkey_hex AS identity_hex,
                       SUBSTR(r.pubkey_hex, 1, 8) AS hash_prefix_hex,
                       COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8)) AS name,
                       'Repeater' AS role,
                       r.last_lat AS latitude,
                       r.last_lon AS longitude,
                       r.last_seen_at AS last_advert_at,
                       r.last_probe_status,
                       r.last_probe_at,
                       (
                           SELECT MAX(ns.observed_at)
                           FROM repeater_probe_runs pr
                           JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                           WHERE pr.repeater_id = r.id
                       ) AS last_data_at,
                       (
                           SELECT MAX(pr.finished_at)
                           FROM repeater_probe_runs pr
                           WHERE pr.repeater_id = r.id AND pr.result = 'success'
                       ) AS last_successful_probe_at,
                       EXISTS(
                           SELECT 1
                           FROM repeater_probe_runs pr
                           JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                           WHERE pr.repeater_id = r.id
                           LIMIT 1
                       ) AS data_fetch_ok
                FROM repeaters r
                ORDER BY r.last_seen_at DESC, r.id DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def latest_repeater_neighbor_links(self, limit_repeaters: int = 64) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                WITH latest_runs AS (
                    SELECT pr.repeater_id, MAX(pr.id) AS probe_run_id
                    FROM repeater_probe_runs pr
                    JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                    GROUP BY pr.repeater_id
                    ORDER BY MAX(pr.id) DESC
                    LIMIT ?
                )
                SELECT src.pubkey_hex AS source_identity_hex,
                       SUBSTR(src.pubkey_hex, 1, 8) AS source_hash_prefix_hex,
                       COALESCE(NULLIF(TRIM(src.last_name_from_advert), ''), SUBSTR(src.pubkey_hex, 1, 8)) AS source_name,
                       src.last_lat AS source_latitude,
                       src.last_lon AS source_longitude,
                       lr.probe_run_id,
                       ns.observed_at AS collected_at,
                       ns.heard_seconds_ago AS last_heard_seconds,
                       ns.snr,
                       ns.neighbour_pubkey_prefix_hex AS target_hash_prefix_hex,
                       COALESCE(
                           (
                               SELECT t.pubkey_hex
                               FROM repeaters t
                               WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                               ORDER BY t.last_seen_at DESC, t.id DESC
                               LIMIT 1
                           ),
                           ns.neighbour_pubkey_prefix_hex
                       ) AS target_identity_hex,
                       COALESCE(
                           (
                               SELECT COALESCE(NULLIF(TRIM(t.last_name_from_advert), ''), SUBSTR(t.pubkey_hex, 1, 8))
                               FROM repeaters t
                               WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                               ORDER BY t.last_seen_at DESC, t.id DESC
                               LIMIT 1
                           ),
                           ns.neighbour_pubkey_prefix_hex
                       ) AS target_name,
                       (
                           SELECT t.last_lat
                           FROM repeaters t
                           WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                           ORDER BY t.last_seen_at DESC, t.id DESC
                           LIMIT 1
                       ) AS target_latitude,
                       (
                           SELECT t.last_lon
                           FROM repeaters t
                           WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                           ORDER BY t.last_seen_at DESC, t.id DESC
                           LIMIT 1
                       ) AS target_longitude
                FROM latest_runs lr
                JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = lr.probe_run_id
                JOIN repeaters src ON src.id = lr.repeater_id
                ORDER BY ns.observed_at DESC, src.last_seen_at DESC, ns.snr DESC, ns.id DESC
                """,
                (limit_repeaters,),
            ).fetchall()
            return [dict(row) for row in rows]

    def repeater_neighbor_signal_history(self, limit_samples_per_source: int = 96) -> dict[str, list[dict[str, object]]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT src.pubkey_hex AS source_identity_hex,
                       ns.observed_at AS collected_at,
                       ns.heard_seconds_ago AS last_heard_seconds,
                       ns.snr,
                       ns.neighbour_pubkey_prefix_hex AS target_hash_prefix_hex,
                       COALESCE(
                           (
                               SELECT t.pubkey_hex
                               FROM repeaters t
                               WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                               ORDER BY t.last_seen_at DESC, t.id DESC
                               LIMIT 1
                           ),
                           ns.neighbour_pubkey_prefix_hex
                       ) AS target_identity_hex
                FROM repeater_neighbour_snapshots ns
                JOIN repeater_probe_runs pr ON pr.id = ns.probe_run_id
                JOIN repeaters src ON src.id = pr.repeater_id
                ORDER BY src.pubkey_hex ASC, ns.observed_at DESC, ns.id DESC
                """
            ).fetchall()

        history: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            source_identity_hex = str(row["source_identity_hex"])
            bucket = history.setdefault(source_identity_hex, [])
            if len(bucket) >= limit_samples_per_source:
                continue
            bucket.append(dict(row))
        return history

    def latest_repeater_signal_by_name(self, name: str) -> dict[str, object] | None:
        normalized_name = name.strip()
        if not normalized_name:
            return None
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT r.id AS repeater_id,
                       r.pubkey_hex,
                       COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8)) AS repeater_name,
                       rss.observed_at,
                       rss.last_snr,
                       rss.last_rssi
                FROM repeaters r
                LEFT JOIN repeater_probe_runs pr
                  ON pr.id = (
                      SELECT pr2.id
                      FROM repeater_probe_runs pr2
                      WHERE pr2.repeater_id = r.id AND pr2.result = 'success'
                      ORDER BY COALESCE(pr2.finished_at, pr2.started_at) DESC, pr2.id DESC
                      LIMIT 1
                  )
                LEFT JOIN repeater_status_snapshots rss
                  ON rss.id = (
                      SELECT rss2.id
                      FROM repeater_status_snapshots rss2
                      WHERE rss2.probe_run_id = pr.id
                      ORDER BY rss2.observed_at DESC, rss2.id DESC
                      LIMIT 1
                  )
                WHERE LOWER(COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8))) = LOWER(?)
                ORDER BY r.last_seen_at DESC, r.id DESC
                LIMIT 1
                """,
                (normalized_name,),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_repeaters(self, *, query: str | None = None, limit: int = 100) -> list[dict[str, object]]:
        normalized_query = (query or "").strip()
        with self.connect() as connection:
            if normalized_query:
                pattern = f"%{normalized_query.lower()}%"
                rows = connection.execute(
                    """
                    SELECT r.id,
                           r.pubkey_hex,
                           COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8)) AS name,
                           r.last_seen_at,
                           r.last_probe_status,
                           r.last_probe_at,
                           r.learned_login_role,
                           r.learned_login_success_count
                    FROM repeaters r
                    WHERE LOWER(COALESCE(r.last_name_from_advert, '')) LIKE ?
                       OR LOWER(r.pubkey_hex) LIKE ?
                       OR CAST(r.id AS TEXT) = ?
                    ORDER BY r.last_seen_at DESC, r.id DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, normalized_query, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT r.id,
                           r.pubkey_hex,
                           COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8)) AS name,
                           r.last_seen_at,
                           r.last_probe_status,
                           r.last_probe_at,
                           r.learned_login_role,
                           r.learned_login_success_count
                    FROM repeaters r
                    ORDER BY r.last_seen_at DESC, r.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            return [dict(row) for row in rows]

    def list_repeaters_seen_on_endpoint(
        self,
        *,
        endpoint_name: str,
        limit: int = 100,
        seen_within_hours: float | None = 24.0,
    ) -> list[dict[str, object]]:
        normalized_endpoint = endpoint_name.strip()
        if not normalized_endpoint:
            return []
        with self.connect() as connection:
            params: list[object] = [normalized_endpoint]
            time_filter = ""
            if seen_within_hours is not None and seen_within_hours > 0:
                cutoff_iso = (datetime.now(tz=UTC) - timedelta(hours=seen_within_hours)).isoformat()
                time_filter = " AND observed_at >= ?"
                params.append(cutoff_iso)
            params.append(limit)
            rows = connection.execute(
                f"""
                SELECT r.id,
                       r.pubkey_hex,
                       COALESCE(NULLIF(TRIM(r.last_name_from_advert), ''), SUBSTR(r.pubkey_hex, 1, 8)) AS name,
                       ra.observed_at AS advert_observed_at,
                       ra.path_len AS advert_path_len,
                       ra.path_hex AS advert_path_hex,
                       ra.advert_name,
                       r.last_probe_status,
                       r.last_probe_at
                FROM repeater_adverts ra
                JOIN (
                    SELECT repeater_id, MAX(id) AS max_id
                    FROM repeater_adverts
                    WHERE endpoint_name = ?{time_filter}
                    GROUP BY repeater_id
                ) latest ON latest.max_id = ra.id
                JOIN repeaters r ON r.id = ra.repeater_id
                ORDER BY ra.observed_at DESC, r.id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]

    def repeater_full_state(self, *, repeater_id: int) -> dict[str, object] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT r.id,
                       r.pubkey_hex,
                       r.first_seen_at,
                       r.last_seen_at,
                       r.last_name_from_advert,
                       r.last_lat,
                       r.last_lon,
                       r.last_advert_timestamp_remote,
                       r.last_guest_permissions,
                       r.last_firmware_capability_level,
                       r.last_login_server_time,
                       r.last_probe_status,
                       r.last_probe_at,
                       r.learned_login_role,
                       r.learned_login_password,
                       r.learned_login_success_count,
                       r.learned_login_updated_at,
                       r.preferred_endpoint_name,
                       r.preferred_endpoint_updated_at,
                       (
                           SELECT MAX(ns.observed_at)
                           FROM repeater_probe_runs pr
                           JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                           WHERE pr.repeater_id = r.id
                       ) AS last_data_at,
                       (
                           SELECT MAX(pr.finished_at)
                           FROM repeater_probe_runs pr
                           WHERE pr.repeater_id = r.id AND pr.result = 'success'
                       ) AS last_successful_probe_at,
                       (
                           SELECT pj.scheduled_at
                           FROM probe_jobs pj
                           WHERE pj.repeater_id = r.id AND pj.status IN ('pending', 'running')
                           ORDER BY pj.scheduled_at ASC, pj.id ASC
                           LIMIT 1
                       ) AS next_probe_scheduled_at,
                       (
                           SELECT pj.reason
                           FROM probe_jobs pj
                           WHERE pj.repeater_id = r.id AND pj.status IN ('pending', 'running')
                           ORDER BY pj.scheduled_at ASC, pj.id ASC
                           LIMIT 1
                       ) AS next_probe_reason,
                       (
                           SELECT pj.status
                           FROM probe_jobs pj
                           WHERE pj.repeater_id = r.id AND pj.status IN ('pending', 'running')
                           ORDER BY pj.scheduled_at ASC, pj.id ASC
                           LIMIT 1
                       ) AS next_probe_status,
                       (
                           SELECT COUNT(*)
                           FROM repeater_adverts ra
                           WHERE ra.repeater_id = r.id
                       ) AS advert_count,
                       (
                           SELECT COUNT(*)
                           FROM repeater_probe_runs pr
                           WHERE pr.repeater_id = r.id
                       ) AS probe_run_count
                FROM repeaters r
                WHERE r.id = ?
                LIMIT 1
                """,
                (repeater_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def repeater_recent_probe_runs(self, *, repeater_id: int, limit: int = 10) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, endpoint_name, started_at, finished_at, result,
                       guest_login_ok, guest_permissions, firmware_capability_level,
                       login_server_time, error_message
                FROM repeater_probe_runs
                WHERE repeater_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (repeater_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def latest_repeater_neighbours(self, *, repeater_id: int, limit: int = 64) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                WITH latest_run AS (
                    SELECT pr.id AS probe_run_id
                    FROM repeater_probe_runs pr
                    JOIN repeater_neighbour_snapshots ns ON ns.probe_run_id = pr.id
                    WHERE pr.repeater_id = ?
                    ORDER BY COALESCE(pr.finished_at, pr.started_at) DESC, pr.id DESC
                    LIMIT 1
                )
                SELECT ns.probe_run_id,
                       ns.observed_at,
                       ns.page_offset,
                       ns.total_neighbours_count,
                       ns.results_count,
                       ns.neighbour_pubkey_prefix_hex,
                       ns.heard_seconds_ago,
                       ns.snr,
                       (
                           SELECT t.pubkey_hex
                           FROM repeaters t
                           WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                           ORDER BY t.last_seen_at DESC, t.id DESC
                           LIMIT 1
                       ) AS resolved_pubkey_hex,
                       (
                           SELECT COALESCE(NULLIF(TRIM(t.last_name_from_advert), ''), SUBSTR(t.pubkey_hex, 1, 8))
                           FROM repeaters t
                           WHERE t.pubkey_hex LIKE ns.neighbour_pubkey_prefix_hex || '%'
                           ORDER BY t.last_seen_at DESC, t.id DESC
                           LIMIT 1
                       ) AS resolved_name
                FROM repeater_neighbour_snapshots ns
                WHERE ns.probe_run_id = (SELECT probe_run_id FROM latest_run)
                ORDER BY ns.snr DESC, ns.heard_seconds_ago ASC, ns.id ASC
                LIMIT ?
                """,
                (repeater_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def probe_jobs_for_repeater(self, *, repeater_id: int, limit: int = 20) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT id, endpoint_name, reason, status, scheduled_at, started_at, finished_at, attempts, last_error
                FROM probe_jobs
                WHERE repeater_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (repeater_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_repeater_metadata(
        self,
        *,
        repeater_id: int,
        name: str | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
    ) -> dict[str, object] | None:
        def operation(connection: sqlite3.Connection) -> dict[str, object] | None:
            row = connection.execute(
                "SELECT id, last_name_from_advert, last_lat, last_lon FROM repeaters WHERE id = ? LIMIT 1",
                (repeater_id,),
            ).fetchone()
            if row is None:
                return None
            next_name = row["last_name_from_advert"] if name is None else name
            next_lat = row["last_lat"] if latitude is None else latitude
            next_lon = row["last_lon"] if longitude is None else longitude
            connection.execute(
                """
                UPDATE repeaters
                SET last_name_from_advert = ?, last_lat = ?, last_lon = ?
                WHERE id = ?
                """,
                (next_name, next_lat, next_lon, repeater_id),
            )
            updated = connection.execute(
                "SELECT id, last_name_from_advert, last_lat, last_lon FROM repeaters WHERE id = ? LIMIT 1",
                (repeater_id,),
            ).fetchone()
            return dict(updated) if updated is not None else None

        return self._run_with_retry(operation)

    def create_manual_repeater(
        self,
        *,
        pubkey_hex: str,
        name: str | None,
        endpoint_name: str,
        latitude: float | None = None,
        longitude: float | None = None,
    ) -> int:
        normalized_pubkey_hex = pubkey_hex.strip().upper()
        public_key = bytes.fromhex(normalized_pubkey_hex)
        now_iso = utc_now_iso()

        def operation(connection: sqlite3.Connection) -> int:
            existing = connection.execute(
                "SELECT id FROM repeaters WHERE pubkey_hex = ? LIMIT 1",
                (normalized_pubkey_hex,),
            ).fetchone()
            if existing is not None:
                repeater_id = int(existing["id"])
                connection.execute(
                    """
                    UPDATE repeaters
                    SET last_name_from_advert = COALESCE(?, last_name_from_advert),
                        last_lat = COALESCE(?, last_lat),
                        last_lon = COALESCE(?, last_lon),
                        last_seen_at = ?
                    WHERE id = ?
                    """,
                    (name, latitude, longitude, now_iso, repeater_id),
                )
                return repeater_id
            cursor = connection.execute(
                """
                INSERT INTO repeaters (
                    pubkey, pubkey_hex, first_seen_at, last_seen_at, last_name_from_advert,
                    last_lat, last_lon, last_advert_timestamp_remote, last_probe_status, last_probe_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                """,
                (public_key, normalized_pubkey_hex, now_iso, now_iso, name, latitude, longitude),
            )
            lastrowid = cursor.lastrowid
            assert lastrowid is not None
            repeater_id = int(lastrowid)
            connection.execute(
                """
                INSERT INTO repeater_adverts (
                    repeater_id, endpoint_name, observed_at, advert_timestamp_remote,
                    advert_name, advert_lat, advert_lon, path_len, path_hex, raw_packet_hex
                ) VALUES (?, ?, ?, 0, ?, ?, ?, 0, '', '')
                """,
                (repeater_id, endpoint_name, now_iso, name, latitude, longitude),
            )
            return repeater_id

        return self._run_with_retry(operation)

    def delete_repeater(self, *, repeater_id: int) -> bool:
        def operation(connection: sqlite3.Connection) -> bool:
            exists = connection.execute("SELECT 1 FROM repeaters WHERE id = ? LIMIT 1", (repeater_id,)).fetchone()
            if exists is None:
                return False
            probe_run_ids = [
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM repeater_probe_runs WHERE repeater_id = ?",
                    (repeater_id,),
                ).fetchall()
            ]
            if probe_run_ids:
                placeholders = ",".join("?" for _ in probe_run_ids)
                connection.execute(f"DELETE FROM repeater_owner_snapshots WHERE probe_run_id IN ({placeholders})", probe_run_ids)
                connection.execute(f"DELETE FROM repeater_status_snapshots WHERE probe_run_id IN ({placeholders})", probe_run_ids)
                connection.execute(f"DELETE FROM repeater_telemetry_snapshots WHERE probe_run_id IN ({placeholders})", probe_run_ids)
                connection.execute(f"DELETE FROM repeater_neighbour_snapshots WHERE probe_run_id IN ({placeholders})", probe_run_ids)
                connection.execute(f"DELETE FROM raw_mesh_packets WHERE probe_run_id IN ({placeholders})", probe_run_ids)
            connection.execute("DELETE FROM probe_jobs WHERE repeater_id = ?", (repeater_id,))
            connection.execute("DELETE FROM repeater_probe_runs WHERE repeater_id = ?", (repeater_id,))
            connection.execute("DELETE FROM repeater_paths WHERE repeater_id = ?", (repeater_id,))
            connection.execute("DELETE FROM repeater_adverts WHERE repeater_id = ?", (repeater_id,))
            connection.execute("DELETE FROM repeaters WHERE id = ?", (repeater_id,))
            return True

        return self._run_with_retry(operation)

    def list_probe_jobs(self, limit: int = 100) -> list[dict[str, object]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT pj.id, pj.endpoint_name, pj.reason, pj.status, pj.scheduled_at,
                       pj.started_at, pj.finished_at, pj.attempts, pj.last_error,
                       r.pubkey_hex, r.last_name_from_advert
                FROM probe_jobs pj
                JOIN repeaters r ON r.id = pj.repeater_id
                ORDER BY pj.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def _scalar(connection: sqlite3.Connection, query: str) -> int | str | None:
        row = connection.execute(query).fetchone()
        return row[0] if row is not None else None

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
        if column_name not in columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    @classmethod
    def _is_retryable_operational_error(cls, exc: sqlite3.OperationalError) -> bool:
        message = str(exc).lower()
        return (
            "database is locked" in message
            or "database is busy" in message
            or "disk i/o error" in message
        )

    def _run_with_retry(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(self.WRITE_RETRY_ATTEMPTS):
            try:
                with self.connect() as connection:
                    return operation(connection)
            except sqlite3.OperationalError as exc:
                last_error = exc
                if not self._is_retryable_operational_error(exc) or attempt == self.WRITE_RETRY_ATTEMPTS - 1:
                    raise
                time.sleep(self.WRITE_RETRY_DELAY_SECS * (attempt + 1))
        assert last_error is not None
        raise last_error
