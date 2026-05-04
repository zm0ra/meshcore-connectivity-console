"""Persistent MeshCore identity handling compatible with upstream Ed25519/ECDH behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

import nacl.bindings
import nacl.utils

PUB_KEY_SIZE = 32
PRV_KEY_SIZE = 64
SEED_SIZE = 32


@dataclass(slots=True)
class MeshcoreIdentity:
    public_key: bytes
    private_key: bytes

    @staticmethod
    def _clamp_scalar(private_key: bytes) -> bytes:
        scalar = bytearray(private_key[:32])
        scalar[0] &= 248
        scalar[31] &= 63
        scalar[31] |= 64
        return bytes(scalar)

    @classmethod
    def _derive_public_key_from_meshcore_private(cls, private_key: bytes) -> bytes:
        return nacl.bindings.crypto_scalarmult_ed25519_base_noclamp(cls._clamp_scalar(private_key))

    @classmethod
    def _expand_seed_to_meshcore_private(cls, seed: bytes) -> bytes:
        expanded = bytearray(hashlib.sha512(seed).digest())
        expanded[0] &= 248
        expanded[31] &= 63
        expanded[31] |= 64
        return bytes(expanded)

    @classmethod
    def generate(cls) -> "MeshcoreIdentity":
        seed = nacl.utils.random(SEED_SIZE)
        private_key = cls._expand_seed_to_meshcore_private(seed)
        public_key = cls._derive_public_key_from_meshcore_private(private_key)
        return cls(public_key=public_key, private_key=private_key)

    @classmethod
    def from_private_key(cls, private_key: bytes, public_key: bytes | None = None) -> "MeshcoreIdentity":
        if len(private_key) == SEED_SIZE:
            expanded_private_key = cls._expand_seed_to_meshcore_private(private_key)
            derived_public_key = cls._derive_public_key_from_meshcore_private(expanded_private_key)
            return cls(public_key=derived_public_key, private_key=expanded_private_key)
        if len(private_key) != PRV_KEY_SIZE:
            raise ValueError(f"private key must be {SEED_SIZE} or {PRV_KEY_SIZE} bytes")

        derived_public_key = cls._derive_public_key_from_meshcore_private(private_key)
        if public_key is not None and len(public_key) != PUB_KEY_SIZE:
            raise ValueError(f"public key must be {PUB_KEY_SIZE} bytes")

        if private_key[32:] == (public_key or private_key[32:]):
            seed = private_key[:32]
            meshcore_private = cls._expand_seed_to_meshcore_private(seed)
            derived_public_key = cls._derive_public_key_from_meshcore_private(meshcore_private)
            if public_key is not None and derived_public_key != public_key:
                raise ValueError("provided public key does not match derived MeshCore key")
            return cls(public_key=derived_public_key, private_key=meshcore_private)

        if public_key is not None and derived_public_key != public_key:
            raise ValueError("provided public key does not match derived MeshCore key")
        return cls(public_key=derived_public_key, private_key=private_key)

    @property
    def public_key_hex(self) -> str:
        return self.public_key.hex()

    @property
    def private_key_hex(self) -> str:
        return self.private_key.hex()

    def hash_prefix_hex(self, length: int = 1) -> str:
        if not 1 <= length <= PUB_KEY_SIZE:
            raise ValueError("hash prefix length must be between 1 and 32")
        return self.public_key[:length].hex().upper()

    def calc_shared_secret(self, peer_public_key: bytes) -> bytes:
        curve_private = self._clamp_scalar(self.private_key)
        curve_public = nacl.bindings.crypto_sign_ed25519_pk_to_curve25519(peer_public_key)
        return nacl.bindings.crypto_scalarmult(curve_private, curve_public)

    def sign(self, message: bytes) -> bytes:
        expanded_private = self.private_key
        if len(expanded_private) != PRV_KEY_SIZE:
            raise ValueError("MeshCore signing requires a 64-byte expanded private key")

        nonce_digest = hashlib.sha512(expanded_private[32:] + message).digest()
        nonce_scalar = nacl.bindings.crypto_core_ed25519_scalar_reduce(nonce_digest)
        encoded_r = nacl.bindings.crypto_scalarmult_ed25519_base_noclamp(nonce_scalar)

        challenge_digest = hashlib.sha512(encoded_r + self.public_key + message).digest()
        challenge_scalar = nacl.bindings.crypto_core_ed25519_scalar_reduce(challenge_digest)
        private_scalar = self._clamp_scalar(expanded_private)
        signature_scalar = nacl.bindings.crypto_core_ed25519_scalar_add(
            nonce_scalar,
            nacl.bindings.crypto_core_ed25519_scalar_mul(challenge_scalar, private_scalar),
        )
        return encoded_r + signature_scalar

    def to_document(self) -> dict[str, str]:
        return {
            "public_key_hex": self.public_key_hex,
            "private_key_hex": self.private_key_hex,
        }


def load_or_create_identity(path: str | Path) -> tuple[MeshcoreIdentity, bool]:
    identity_path = Path(path).expanduser().resolve()
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    if identity_path.exists():
        document = json.loads(identity_path.read_text(encoding="utf-8"))
        identity = MeshcoreIdentity.from_private_key(
            bytes.fromhex(document["private_key_hex"]),
            bytes.fromhex(document["public_key_hex"]),
        )
        return identity, False

    identity = MeshcoreIdentity.generate()
    save_identity(path, identity)
    return identity, True


def save_identity(path: str | Path, identity: MeshcoreIdentity) -> None:
    identity_path = Path(path).expanduser().resolve()
    identity_path.parent.mkdir(parents=True, exist_ok=True)
    identity_path.write_text(
        json.dumps(
            {
                **identity.to_document(),
                "created_at": datetime.now(tz=UTC).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )