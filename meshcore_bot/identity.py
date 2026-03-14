from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nacl import bindings
from nacl.signing import SigningKey


PUBLIC_KEY_SIZE = 32
PRIVATE_KEY_SIZE = 64
IDENTITY_BLOB_SIZE = PRIVATE_KEY_SIZE + PUBLIC_KEY_SIZE


@dataclass(slots=True)
class LocalIdentity:
    private_key: bytes
    public_key: bytes

    @classmethod
    def generate(cls) -> "LocalIdentity":
        public_key, private_key = bindings.crypto_sign_keypair()
        return cls(private_key=private_key, public_key=public_key)

    @classmethod
    def load_or_create(cls, path: str | Path) -> "LocalIdentity":
        file_path = Path(path).expanduser().resolve()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists():
            blob = file_path.read_bytes()
            return cls.from_bytes(blob)
        identity = cls.generate()
        file_path.write_bytes(identity.to_bytes())
        return identity

    @classmethod
    def from_bytes(cls, blob: bytes) -> "LocalIdentity":
        if len(blob) == IDENTITY_BLOB_SIZE:
            private_key = blob[:PRIVATE_KEY_SIZE]
            public_key = blob[PRIVATE_KEY_SIZE:]
            return cls(private_key=private_key, public_key=public_key)
        if len(blob) == PRIVATE_KEY_SIZE:
            private_key = blob
            public_key = private_key[32:]
            return cls(private_key=private_key, public_key=public_key)
        raise ValueError("identity blob must be 64 or 96 bytes")

    def to_bytes(self) -> bytes:
        return self.private_key + self.public_key

    def public_hash(self, length: int = 1) -> bytes:
        return self.public_key[:length]

    def calc_shared_secret(self, other_public_key: bytes) -> bytes:
        curve_private = bindings.crypto_sign_ed25519_sk_to_curve25519(self.private_key)
        curve_public = bindings.crypto_sign_ed25519_pk_to_curve25519(other_public_key)
        return bindings.crypto_scalarmult(curve_private, curve_public)

    def sign(self, message: bytes) -> bytes:
        return SigningKey(self.private_key[:32]).sign(message).signature
