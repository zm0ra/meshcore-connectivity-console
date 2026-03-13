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


def _default_bot_runtime_settings(config: AppConfig) -> dict[str, Any]:
    return {
        "name": config.bot.name,
        "reply_prefix": config.bot.reply_prefix,
        "command_prefix": config.bot.command_prefix,
        "message_history_size": config.bot.message_history_size,
        "signal_history_limit": 32,
        "signal_history_target_limit": 12,
        "neighbor_snapshot_retention": 96,
        "private_messages_enabled": True,
        "private_message_auto_response": (
            f"{config.bot.reply_prefix}Private messages are enabled. Try {config.bot.command_prefix}help"
        ),
    }


def _default_command_settings() -> dict[str, dict[str, Any]]:
    return {
        "ping": {"enabled": True, "response_template": "pong", "sort_order": 10},
        "help": {
            "enabled": True,
            "response_template": "{reply_prefix}Commands: {command_list}",
            "sort_order": 20,
        },
        "test": {
            "enabled": True,
            "response_template": (
                "{reply_prefix}I saw: {sender} "
                "(hops={path_len}{snr_suffix}{rssi_suffix}{distance_suffix})"
            ),
            "sort_order": 30,
        },
        "trace": {
            "enabled": True,
            "response_template": "{reply_prefix}Trace: {trace}",
            "sort_order": 40,
        },
        "neighbors": {
            "enabled": True,
            "response_template": "{reply_prefix}{neighbors_summary}",
            "sort_order": 50,
        },
    }


class BotDatabase:
    SCHEMA_VERSION = 2

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

                CREATE TABLE IF NOT EXISTS bot_runtime_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    bot_name TEXT NOT NULL,
                    reply_prefix TEXT NOT NULL,
                    command_prefix TEXT NOT NULL,
                    message_history_size INTEGER NOT NULL DEFAULT 200,
                    signal_history_limit INTEGER NOT NULL DEFAULT 32,
                    signal_history_target_limit INTEGER NOT NULL DEFAULT 12,
                    neighbor_snapshot_retention INTEGER NOT NULL DEFAULT 96,
                    private_messages_enabled INTEGER NOT NULL DEFAULT 1,
                    private_message_auto_response TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_command_settings (
                    command_name TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    response_template TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_identity_cache (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    identity_file_path TEXT,
                    public_key_hex TEXT,
                    hash_prefix_hex TEXT,
                    last_loaded_at TEXT,
                    last_rotated_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'admin',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_login_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admin_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_username TEXT,
                    action TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_key TEXT,
                    remote_addr TEXT,
                    payload_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(actor_username) REFERENCES admin_users(username)
                );

                CREATE INDEX IF NOT EXISTS idx_admin_audit_created_at ON admin_audit_log(created_at DESC);

                CREATE TABLE IF NOT EXISTS endpoint_runtime_state (
                    endpoint_name TEXT PRIMARY KEY,
                    connected INTEGER NOT NULL DEFAULT 0,
                    last_connect_at TEXT,
                    last_disconnect_at TEXT,
                    last_seen_at TEXT,
                    last_error TEXT,
                    last_cli_command TEXT,
                    last_cli_reply TEXT,
                    last_cli_error TEXT,
                    recent_console_lines_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS management_runtime_state (
                    target_name TEXT PRIMARY KEY,
                    endpoint_name TEXT NOT NULL,
                    resolved_identity_hex TEXT,
                    current_role TEXT,
                    pending_login_role TEXT,
                    pending_request TEXT,
                    last_login_at TEXT,
                    last_status_at TEXT,
                    last_status_size INTEGER,
                    last_neighbors_at TEXT,
                    last_owner_at TEXT,
                    last_acl_at TEXT,
                    neighbor_count INTEGER NOT NULL DEFAULT 0,
                    acl_entry_count INTEGER NOT NULL DEFAULT 0,
                    owner_info_json TEXT,
                    last_error TEXT,
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
            self._ensure_schema_defaults(connection, now_iso)

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
            if not self._table_has_rows(connection, "config_channels"):
                for channel in config.channels:
                    self._insert_channel(connection, channel, now_iso)

            if not self._table_has_rows(connection, "config_endpoints"):
                for endpoint in config.endpoints:
                    self._insert_endpoint(connection, endpoint, now_iso)

            if not self._table_has_rows(connection, "management_targets"):
                for target in config.management_targets:
                    self._insert_management_target(connection, target, now_iso)

            self._ensure_runtime_settings(connection, config, now_iso)
            self._ensure_command_settings(connection, now_iso)
            self._ensure_web_setting(connection, config, now_iso)

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
                "bot_runtime_configured": bool(
                    self._scalar(connection, "SELECT COUNT(*) FROM bot_runtime_settings")
                ),
                "command_setting_count": self._scalar(connection, "SELECT COUNT(*) FROM bot_command_settings"),
                "config_channel_count": self._scalar(connection, "SELECT COUNT(*) FROM config_channels"),
                "config_endpoint_count": self._scalar(connection, "SELECT COUNT(*) FROM config_endpoints"),
                "management_target_count": self._scalar(connection, "SELECT COUNT(*) FROM management_targets"),
                "admin_user_count": self._scalar(connection, "SELECT COUNT(*) FROM admin_users"),
                "admin_audit_count": self._scalar(connection, "SELECT COUNT(*) FROM admin_audit_log"),
                "endpoint_runtime_state_count": self._scalar(connection, "SELECT COUNT(*) FROM endpoint_runtime_state"),
                "management_runtime_state_count": self._scalar(connection, "SELECT COUNT(*) FROM management_runtime_state"),
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

    def _table_has_rows(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return row is not None

    def _ensure_schema_defaults(self, connection: sqlite3.Connection, updated_at: str) -> None:
        connection.execute(
            """
            INSERT INTO bot_identity_cache (id, updated_at)
            VALUES (1, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (updated_at,),
        )

    def _ensure_runtime_settings(
        self,
        connection: sqlite3.Connection,
        config: AppConfig,
        updated_at: str,
    ) -> None:
        if self._table_has_rows(connection, "bot_runtime_settings"):
            return

        defaults = _default_bot_runtime_settings(config)
        connection.execute(
            """
            INSERT INTO bot_runtime_settings (
                id, bot_name, reply_prefix, command_prefix, message_history_size,
                signal_history_limit, signal_history_target_limit, neighbor_snapshot_retention,
                private_messages_enabled, private_message_auto_response, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                defaults["name"],
                defaults["reply_prefix"],
                defaults["command_prefix"],
                defaults["message_history_size"],
                defaults["signal_history_limit"],
                defaults["signal_history_target_limit"],
                defaults["neighbor_snapshot_retention"],
                1 if defaults["private_messages_enabled"] else 0,
                defaults["private_message_auto_response"],
                updated_at,
            ),
        )
        self._set_json_setting(connection, "runtime.bot", defaults, updated_at)

    def _ensure_command_settings(self, connection: sqlite3.Connection, updated_at: str) -> None:
        if self._table_has_rows(connection, "bot_command_settings"):
            return

        defaults = _default_command_settings()
        for command_name, settings in defaults.items():
            connection.execute(
                """
                INSERT INTO bot_command_settings (
                    command_name, enabled, response_template, sort_order, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    command_name,
                    1 if settings["enabled"] else 0,
                    settings["response_template"],
                    settings["sort_order"],
                    updated_at,
                ),
            )
        self._set_json_setting(connection, "runtime.commands", defaults, updated_at)

    def _ensure_web_setting(
        self,
        connection: sqlite3.Connection,
        config: AppConfig,
        updated_at: str,
    ) -> None:
        existing = connection.execute(
            "SELECT 1 FROM app_settings WHERE key = 'runtime.web'"
        ).fetchone()
        if existing is not None:
            return

        self._set_json_setting(
            connection,
            "runtime.web",
            {
                "host": config.web.host,
                "port": config.web.port,
            },
            updated_at,
        )

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