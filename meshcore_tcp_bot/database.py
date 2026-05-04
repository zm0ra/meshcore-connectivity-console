"""SQLite persistence for adverts, management snapshots, and topology state."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import sqlite3
from pathlib import Path
from threading import RLock
from typing import Any

from .models import NodeRecord
from .packets import AdvertRecord


REPEATER_ROLES = {"Repeater", "Room Server"}


def _is_repeater_role(role: str | None) -> bool:
    return role in REPEATER_ROLES


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


class MeshcoreStore:
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

                CREATE TABLE IF NOT EXISTS nodes (
                    identity_hex TEXT PRIMARY KEY,
                    hash_prefix_hex TEXT NOT NULL,
                    name TEXT,
                    role TEXT,
                    latitude REAL,
                    longitude REAL,
                    last_advert_timestamp INTEGER,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_seen_endpoint TEXT
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
                    FOREIGN KEY(identity_hex) REFERENCES nodes(identity_hex)
                );

                CREATE INDEX IF NOT EXISTS idx_advert_history_identity ON advert_history(identity_hex);
                CREATE INDEX IF NOT EXISTS idx_advert_history_heard_at ON advert_history(heard_at DESC);

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

                CREATE TABLE IF NOT EXISTS neighbor_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    requester_role TEXT,
                    collected_at TEXT NOT NULL,
                    success INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_neighbor_snapshots_target ON neighbor_snapshots(target_name, collected_at DESC);

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

                CREATE INDEX IF NOT EXISTS idx_neighbor_edges_snapshot ON neighbor_edges(snapshot_id);

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

                CREATE INDEX IF NOT EXISTS idx_owner_snapshots_target ON owner_snapshots(target_name, collected_at DESC);

                CREATE TABLE IF NOT EXISTS acl_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_name TEXT NOT NULL,
                    endpoint_name TEXT NOT NULL,
                    requester_role TEXT,
                    collected_at TEXT NOT NULL,
                    success INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_acl_snapshots_target ON acl_snapshots(target_name, collected_at DESC);

                CREATE TABLE IF NOT EXISTS acl_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL,
                    pubkey_prefix_hex TEXT NOT NULL,
                    permissions INTEGER NOT NULL,
                    FOREIGN KEY(snapshot_id) REFERENCES acl_snapshots(id)
                );

                CREATE INDEX IF NOT EXISTS idx_acl_entries_snapshot ON acl_entries(snapshot_id);

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self.prune_non_repeater_data()

    def has_management_targets(self) -> bool:
        with self._lock, self._connect() as connection:
            row = connection.execute("SELECT 1 FROM management_targets LIMIT 1").fetchone()
        return row is not None

    def ensure_json_setting(self, key: str, default_value: Any) -> Any:
        existing = self.get_json_setting(key)
        if existing is not None:
            return existing
        self.set_json_setting(key, default_value)
        return default_value

    def get_json_setting(self, key: str) -> Any | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                "SELECT value_json FROM app_settings WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def set_json_setting(self, key: str, value: Any) -> None:
        value_json = json.dumps(value, ensure_ascii=True, sort_keys=True)
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO app_settings (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, value_json, now_iso),
            )

    def list_json_settings(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                "SELECT key, value_json FROM app_settings ORDER BY key"
            ).fetchall()
        return {str(row["key"]): json.loads(row["value_json"]) for row in rows}

    def load_nodes(self) -> list[NodeRecord]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT identity_hex, hash_prefix_hex, name, role, latitude, longitude,
                       last_seen_at, last_seen_endpoint
                FROM nodes
                WHERE role IN ('Repeater', 'Room Server')
                ORDER BY COALESCE(name, identity_hex)
                """
            ).fetchall()

        records: list[NodeRecord] = []
        for row in rows:
            last_advert_at = None
            if row["last_seen_at"]:
                last_advert_at = datetime.fromisoformat(row["last_seen_at"])
            records.append(
                NodeRecord(
                    identity_hex=row["identity_hex"],
                    hash_prefix_hex=row["hash_prefix_hex"],
                    name=row["name"],
                    role=row["role"],
                    latitude=row["latitude"],
                    longitude=row["longitude"],
                    last_advert_at=last_advert_at,
                    last_seen_endpoint=row["last_seen_endpoint"],
                )
            )
        return records

    def upsert_advert(self, endpoint_name: str, advert: AdvertRecord, heard_at: datetime) -> None:
        if not _is_repeater_role(advert.role):
            return
        heard_at_iso = heard_at.astimezone(UTC).isoformat()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO nodes (
                    identity_hex, hash_prefix_hex, name, role, latitude, longitude,
                    last_advert_timestamp, first_seen_at, last_seen_at, last_seen_endpoint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(identity_hex) DO UPDATE SET
                    hash_prefix_hex = excluded.hash_prefix_hex,
                    name = COALESCE(excluded.name, nodes.name),
                    role = COALESCE(excluded.role, nodes.role),
                    latitude = COALESCE(excluded.latitude, nodes.latitude),
                    longitude = COALESCE(excluded.longitude, nodes.longitude),
                    last_advert_timestamp = COALESCE(excluded.last_advert_timestamp, nodes.last_advert_timestamp),
                    last_seen_at = excluded.last_seen_at,
                    last_seen_endpoint = excluded.last_seen_endpoint
                """,
                (
                    advert.identity_hex,
                    advert.hash_prefix_hex,
                    advert.name,
                    advert.role,
                    advert.latitude,
                    advert.longitude,
                    advert.timestamp,
                    heard_at_iso,
                    heard_at_iso,
                    endpoint_name,
                ),
            )
            connection.execute(
                """
                INSERT INTO advert_history (
                    identity_hex, endpoint_name, heard_at, advertised_at, name, role, latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    advert.identity_hex,
                    endpoint_name,
                    heard_at_iso,
                    advert.timestamp,
                    advert.name,
                    advert.role,
                    advert.latitude,
                    advert.longitude,
                ),
            )

    def prune_non_repeater_data(self) -> None:
        with self._lock, self._connect() as connection:
            repeater_roles = tuple(sorted(REPEATER_ROLES))
            connection.execute(
                f"DELETE FROM advert_history WHERE role NOT IN ({','.join('?' for _ in repeater_roles)}) OR role IS NULL",
                repeater_roles,
            )
            connection.execute(
                f"DELETE FROM nodes WHERE role NOT IN ({','.join('?' for _ in repeater_roles)}) OR role IS NULL",
                repeater_roles,
            )

    def sync_management_targets(self, targets: list[dict[str, Any]]) -> None:
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as connection:
            for target in targets:
                connection.execute(
                    """
                    INSERT INTO management_targets (
                        name, endpoint_name, target_hash_prefix, target_identity_hex,
                        guest_password, admin_password, prefer_role, enabled, notes, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        endpoint_name = excluded.endpoint_name,
                        target_hash_prefix = excluded.target_hash_prefix,
                        target_identity_hex = excluded.target_identity_hex,
                        guest_password = excluded.guest_password,
                        admin_password = excluded.admin_password,
                        prefer_role = excluded.prefer_role,
                        enabled = excluded.enabled,
                        notes = excluded.notes,
                        updated_at = excluded.updated_at
                    """,
                    (
                        target["name"],
                        target["endpoint_name"],
                        target.get("target_hash_prefix"),
                        target.get("target_identity_hex"),
                        target.get("guest_password"),
                        target.get("admin_password"),
                        target.get("prefer_role", "guest"),
                        1 if target.get("enabled", True) else 0,
                        target.get("notes"),
                        now_iso,
                    ),
                )

    def upsert_management_target(self, target: dict[str, Any]) -> None:
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO management_targets (
                    name, endpoint_name, target_hash_prefix, target_identity_hex,
                    guest_password, admin_password, prefer_role, enabled, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    endpoint_name = excluded.endpoint_name,
                    target_hash_prefix = COALESCE(excluded.target_hash_prefix, management_targets.target_hash_prefix),
                    target_identity_hex = COALESCE(excluded.target_identity_hex, management_targets.target_identity_hex),
                    guest_password = COALESCE(excluded.guest_password, management_targets.guest_password),
                    admin_password = COALESCE(excluded.admin_password, management_targets.admin_password),
                    prefer_role = excluded.prefer_role,
                    enabled = excluded.enabled,
                    notes = COALESCE(excluded.notes, management_targets.notes),
                    updated_at = excluded.updated_at
                """,
                (
                    target["name"],
                    target["endpoint_name"],
                    target.get("target_hash_prefix"),
                    target.get("target_identity_hex"),
                    target.get("guest_password"),
                    target.get("admin_password"),
                    target.get("prefer_role", "guest"),
                    1 if target.get("enabled", True) else 0,
                    target.get("notes"),
                    now_iso,
                ),
            )

    def rename_management_target(self, old_name: str, new_name: str) -> None:
        if old_name == new_name:
            return
        now_iso = _utc_now_iso()
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT name, endpoint_name, target_hash_prefix, target_identity_hex,
                       guest_password, admin_password, prefer_role, enabled, notes
                FROM management_targets
                WHERE name = ?
                """,
                (old_name,),
            ).fetchone()
            if row is None:
                return
            connection.execute(
                """
                INSERT INTO management_targets (
                    name, endpoint_name, target_hash_prefix, target_identity_hex,
                    guest_password, admin_password, prefer_role, enabled, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    endpoint_name = COALESCE(management_targets.endpoint_name, excluded.endpoint_name),
                    target_hash_prefix = COALESCE(management_targets.target_hash_prefix, excluded.target_hash_prefix),
                    target_identity_hex = COALESCE(management_targets.target_identity_hex, excluded.target_identity_hex),
                    guest_password = COALESCE(management_targets.guest_password, excluded.guest_password),
                    admin_password = COALESCE(management_targets.admin_password, excluded.admin_password),
                    prefer_role = COALESCE(management_targets.prefer_role, excluded.prefer_role),
                    enabled = CASE WHEN management_targets.enabled = 1 OR excluded.enabled = 1 THEN 1 ELSE 0 END,
                    notes = COALESCE(management_targets.notes, excluded.notes),
                    updated_at = excluded.updated_at
                """,
                (
                    new_name,
                    row["endpoint_name"],
                    row["target_hash_prefix"],
                    row["target_identity_hex"],
                    row["guest_password"],
                    row["admin_password"],
                    row["prefer_role"],
                    row["enabled"],
                    row["notes"],
                    now_iso,
                ),
            )
            for table_name in ("neighbor_snapshots", "owner_snapshots", "acl_snapshots"):
                connection.execute(
                    f"UPDATE {table_name} SET target_name = ? WHERE target_name = ?",
                    (new_name, old_name),
                )
            connection.execute("DELETE FROM management_targets WHERE name = ?", (old_name,))

    def delete_management_target(self, name: str) -> None:
        with self._lock, self._connect() as connection:
            connection.execute("DELETE FROM management_targets WHERE name = ?", (name,))

    def list_management_targets(self, *, include_disabled: bool = True) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            if include_disabled:
                rows = connection.execute(
                    """
                    SELECT name, endpoint_name, target_hash_prefix, target_identity_hex,
                           guest_password, admin_password, prefer_role, enabled, notes, updated_at
                    FROM management_targets
                    ORDER BY name COLLATE NOCASE
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT name, endpoint_name, target_hash_prefix, target_identity_hex,
                           guest_password, admin_password, prefer_role, enabled, notes, updated_at
                    FROM management_targets
                    WHERE enabled = 1
                    ORDER BY name COLLATE NOCASE
                    """
                ).fetchall()
        return [dict(row) for row in rows]

    def recent_repeaters(self, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT identity_hex, hash_prefix_hex, name, role, latitude, longitude,
                       last_seen_at, last_seen_endpoint
                FROM nodes
                WHERE role IN ('Repeater', 'Room Server')
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recent_neighbor_summary(self, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ns.target_name,
                       ns.endpoint_name,
                       ns.requester_role,
                       ns.collected_at,
                       ns.success,
                       ns.error_text,
                       COUNT(ne.id) AS neighbor_count
                FROM neighbor_snapshots ns
                LEFT JOIN neighbor_edges ne ON ne.snapshot_id = ns.id
                GROUP BY ns.id
                ORDER BY ns.collected_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_neighbor_snapshot(
        self,
        *,
        target_name: str,
        endpoint_name: str,
        requester_role: str | None,
        success: bool,
        error_text: str | None,
        neighbors: list[dict[str, Any]],
        collected_at: datetime | None = None,
    ) -> None:
        collected_at_iso = (collected_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO neighbor_snapshots (
                    target_name, endpoint_name, requester_role, collected_at, success, error_text
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    target_name,
                    endpoint_name,
                    requester_role,
                    collected_at_iso,
                    1 if success else 0,
                    error_text,
                ),
            )
            snapshot_id = cursor.lastrowid
            for neighbor in neighbors:
                connection.execute(
                    """
                    INSERT INTO neighbor_edges (
                        snapshot_id, neighbor_hash_prefix, neighbor_identity_hex, snr, rssi,
                        last_heard_seconds, label
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        neighbor.get("neighbor_hash_prefix"),
                        neighbor.get("neighbor_identity_hex"),
                        neighbor.get("snr"),
                        neighbor.get("rssi"),
                        neighbor.get("last_heard_seconds"),
                        neighbor.get("label"),
                    ),
                )

    def record_owner_snapshot(
        self,
        *,
        target_name: str,
        endpoint_name: str,
        requester_role: str | None,
        firmware_version: str | None,
        node_name: str | None,
        owner_info: str | None,
        raw_text: str,
        collected_at: datetime | None = None,
    ) -> None:
        collected_at_iso = (collected_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO owner_snapshots (
                    target_name, endpoint_name, requester_role, collected_at,
                    firmware_version, node_name, owner_info, raw_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_name,
                    endpoint_name,
                    requester_role,
                    collected_at_iso,
                    firmware_version,
                    node_name,
                    owner_info,
                    raw_text,
                ),
            )

    def record_acl_snapshot(
        self,
        *,
        target_name: str,
        endpoint_name: str,
        requester_role: str | None,
        success: bool,
        error_text: str | None,
        entries: list[dict[str, Any]],
        collected_at: datetime | None = None,
    ) -> None:
        collected_at_iso = (collected_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO acl_snapshots (
                    target_name, endpoint_name, requester_role, collected_at, success, error_text
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    target_name,
                    endpoint_name,
                    requester_role,
                    collected_at_iso,
                    1 if success else 0,
                    error_text,
                ),
            )
            snapshot_id = cursor.lastrowid
            for entry in entries:
                connection.execute(
                    """
                    INSERT INTO acl_entries (snapshot_id, pubkey_prefix_hex, permissions)
                    VALUES (?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        entry.get("pubkey_prefix_hex"),
                        entry.get("permissions"),
                    ),
                )

    def recent_owner_summary(self, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT target_name, endpoint_name, requester_role, collected_at,
                       firmware_version, node_name, owner_info, raw_text
                FROM owner_snapshots
                ORDER BY collected_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recent_acl_summary(self, limit: int = 8) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT acl.target_name,
                       acl.endpoint_name,
                       acl.requester_role,
                       acl.collected_at,
                       acl.success,
                       acl.error_text,
                       COUNT(ae.id) AS entry_count
                FROM acl_snapshots acl
                LEFT JOIN acl_entries ae ON ae.snapshot_id = acl.id
                GROUP BY acl.id
                ORDER BY acl.collected_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_neighbor_details(self, limit_targets: int = 12) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            snapshots = connection.execute(
                """
                SELECT ns.id, ns.target_name, ns.endpoint_name, ns.requester_role, ns.collected_at
                FROM neighbor_snapshots ns
                JOIN (
                    SELECT target_name, MAX(collected_at) AS latest_collected_at
                    FROM neighbor_snapshots
                    WHERE success = 1
                    GROUP BY target_name
                ) latest
                  ON latest.target_name = ns.target_name
                 AND latest.latest_collected_at = ns.collected_at
                ORDER BY ns.collected_at DESC
                LIMIT ?
                """,
                (limit_targets,),
            ).fetchall()

            result: list[dict[str, Any]] = []
            for snapshot in snapshots:
                edges = connection.execute(
                    """
                    SELECT neighbor_hash_prefix, neighbor_identity_hex, snr, rssi,
                           last_heard_seconds, label
                    FROM neighbor_edges
                    WHERE snapshot_id = ?
                    ORDER BY snr DESC, last_heard_seconds ASC
                    """,
                    (snapshot["id"],),
                ).fetchall()
                result.append(
                    {
                        "snapshot_id": snapshot["id"],
                        "target_name": snapshot["target_name"],
                        "endpoint_name": snapshot["endpoint_name"],
                        "requester_role": snapshot["requester_role"],
                        "collected_at": snapshot["collected_at"],
                        "edges": [dict(row) for row in edges],
                    }
                )
        return result

    def neighbor_signal_history(self, target_name: str, limit_snapshots: int = 32) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT ns.collected_at,
                       ne.neighbor_hash_prefix,
                       ne.neighbor_identity_hex,
                       ne.snr,
                       ne.rssi,
                       ne.last_heard_seconds,
                       ne.label
                FROM neighbor_snapshots ns
                JOIN neighbor_edges ne ON ne.snapshot_id = ns.id
                WHERE ns.target_name = ?
                  AND ns.success = 1
                ORDER BY ns.collected_at DESC, COALESCE(ne.neighbor_identity_hex, ne.neighbor_hash_prefix)
                LIMIT ?
                """,
                (target_name, max(1, limit_snapshots) * 24),
            ).fetchall()
        return [dict(row) for row in rows]

    def prune_neighbor_history(self, target_name: str, keep_snapshots: int) -> None:
        if keep_snapshots < 1:
            keep_snapshots = 1
        with self._lock, self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM neighbor_snapshots
                WHERE target_name = ?
                ORDER BY collected_at DESC
                LIMIT -1 OFFSET ?
                """,
                (target_name, keep_snapshots),
            ).fetchall()
            snapshot_ids = [int(row["id"]) for row in rows]
            if not snapshot_ids:
                return
            placeholders = ",".join("?" for _ in snapshot_ids)
            connection.execute(
                f"DELETE FROM neighbor_edges WHERE snapshot_id IN ({placeholders})",
                snapshot_ids,
            )
            connection.execute(
                f"DELETE FROM neighbor_snapshots WHERE id IN ({placeholders})",
                snapshot_ids,
            )

    def latest_successful_neighbor_snapshot_at(self, target_name: str) -> datetime | None:
        with self._lock, self._connect() as connection:
            row = connection.execute(
                """
                SELECT collected_at
                FROM neighbor_snapshots
                WHERE target_name = ?
                  AND success = 1
                ORDER BY collected_at DESC
                LIMIT 1
                """,
                (target_name,),
            ).fetchone()
        if row is None or not row[0]:
            return None
        return datetime.fromisoformat(row[0]).astimezone(UTC)

    def snapshot_overview(self) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            node_count = connection.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            advert_count = connection.execute("SELECT COUNT(*) FROM advert_history").fetchone()[0]
            target_count = connection.execute("SELECT COUNT(*) FROM management_targets WHERE enabled = 1").fetchone()[0]
            neighbor_snapshot_count = connection.execute("SELECT COUNT(*) FROM neighbor_snapshots").fetchone()[0]
            owner_snapshot_count = connection.execute("SELECT COUNT(*) FROM owner_snapshots").fetchone()[0]
            acl_snapshot_count = connection.execute("SELECT COUNT(*) FROM acl_snapshots").fetchone()[0]
        return {
            "database_path": str(self.database_path),
            "node_count": node_count,
            "advert_count": advert_count,
            "management_target_count": target_count,
            "neighbor_snapshot_count": neighbor_snapshot_count,
            "owner_snapshot_count": owner_snapshot_count,
            "acl_snapshot_count": acl_snapshot_count,
        }