"""Management target registry and dynamic remote-management target tracking."""

from __future__ import annotations

from dataclasses import asdict

from .config import ManagementNodeConfig
from .database import MeshcoreStore


class RepeaterManagementRegistry:
    """Keeps configured management targets synchronized with persistent storage.

    Full MeshCore guest/admin management sessions require a dedicated identity keypair and
    support for ANON_REQ/REQ/RESPONSE packet crypto. This registry prepares configuration
    and persistence now so active management polling can be added without reworking the bot.
    """

    def __init__(self, store: MeshcoreStore, targets: tuple[ManagementNodeConfig, ...]) -> None:
        self.store = store
        self.targets = {item.name: item for item in targets}
        if not self.store.has_management_targets() and targets:
            self.store.sync_management_targets([asdict(item) for item in targets])
        else:
            self.targets = {}
        for persisted in self.store.list_management_targets(include_disabled=True):
            canonical_name = self._canonical_auto_target_name(
                name=str(persisted["name"]),
                endpoint_name=str(persisted["endpoint_name"]),
                notes=str(persisted["notes"]) if persisted.get("notes") is not None else None,
            )
            if canonical_name != persisted["name"]:
                self.store.rename_management_target(str(persisted["name"]), canonical_name)
                persisted = dict(persisted)
                persisted["name"] = canonical_name
            if persisted.get("notes") == "Auto-discovered from repeater advert" and persisted.get("prefer_role") != "guest":
                persisted = dict(persisted)
                persisted["prefer_role"] = "guest"
                self.store.upsert_management_target(persisted)
            if persisted["name"] in self.targets:
                continue
            self.targets[persisted["name"]] = ManagementNodeConfig(
                name=str(persisted["name"]),
                endpoint_name=str(persisted["endpoint_name"]),
                target_hash_prefix=str(persisted["target_hash_prefix"]).upper() if persisted.get("target_hash_prefix") else None,
                target_identity_hex=str(persisted["target_identity_hex"]).lower() if persisted.get("target_identity_hex") else None,
                guest_password=str(persisted["guest_password"]) if persisted.get("guest_password") is not None else None,
                admin_password=str(persisted["admin_password"]) if persisted.get("admin_password") is not None else None,
                prefer_role=str(persisted.get("prefer_role", "guest")).lower(),
                enabled=bool(persisted.get("enabled", True)),
                notes=str(persisted["notes"]) if persisted.get("notes") is not None else None,
            )

    @staticmethod
    def _canonical_auto_target_name(name: str, endpoint_name: str, notes: str | None) -> str:
        if notes != "Auto-discovered from repeater advert":
            return name
        prefix = f"{endpoint_name}:"
        if name.startswith(prefix):
            trimmed = name[len(prefix):].strip()
            return trimmed or name
        return name

    def register_dynamic_target(self, target: ManagementNodeConfig) -> ManagementNodeConfig:
        existing = self.targets.get(target.name)
        if existing is not None:
            merged = ManagementNodeConfig(
                name=existing.name,
                endpoint_name=existing.endpoint_name,
                target_hash_prefix=existing.target_hash_prefix or target.target_hash_prefix,
                target_identity_hex=existing.target_identity_hex or target.target_identity_hex,
                guest_password=existing.guest_password or target.guest_password,
                admin_password=existing.admin_password or target.admin_password,
                prefer_role="guest" if (existing.notes or target.notes) == "Auto-discovered from repeater advert" else existing.prefer_role,
                enabled=existing.enabled,
                notes=existing.notes or target.notes,
            )
            self.targets[target.name] = merged
            self.store.upsert_management_target(asdict(merged))
            return merged

        self.targets[target.name] = target
        self.store.upsert_management_target(asdict(target))
        return target

    def upsert_target(self, target: ManagementNodeConfig, *, old_name: str | None = None) -> ManagementNodeConfig:
        if old_name and old_name != target.name and old_name in self.targets:
            self.store.rename_management_target(old_name, target.name)
            self.targets.pop(old_name, None)
        self.targets[target.name] = target
        self.store.upsert_management_target(asdict(target))
        return target

    def delete_target(self, name: str) -> None:
        self.targets.pop(name, None)
        self.store.delete_management_target(name)

    def list_targets(self) -> list[dict[str, object]]:
        return self.store.list_management_targets(include_disabled=True)

    def get_targets(self) -> list[ManagementNodeConfig]:
        return [item for item in self.targets.values() if item.enabled]
