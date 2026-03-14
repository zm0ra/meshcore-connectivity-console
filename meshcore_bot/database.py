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
    SCHEMA_VERSION = 2
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
    ) -> int | None:
        scheduled_at = utc_now_iso()
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
                (repeater_id, endpoint_name, reason, scheduled_at),
            )
            lastrowid = cursor.lastrowid
            assert lastrowid is not None
            return int(lastrowid)

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
            job_id = self.enqueue_probe_job(
                repeater_id=int(row["id"]),
                endpoint_name=str(row["endpoint_name"]),
                reason=reason,
                success_cooldown_secs=success_cooldown_secs,
                failure_cooldown_secs=failure_cooldown_secs,
            )
            if job_id is not None:
                enqueued += 1
        return enqueued

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
                ORDER BY pj.scheduled_at ASC, pj.id ASC
                LIMIT 1
                """
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
                SET status = 'pending', started_at = NULL, last_error = COALESCE(last_error, 'recovered after worker restart')
                WHERE status = 'running'
                """
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
            return {"jobs_requeued": running_jobs, "runs_interrupted": running_runs}

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
