from __future__ import annotations

import hashlib
import hmac
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


CIPHER_BLOCK_SIZE = 16
CIPHER_KEY_SIZE = 16
CIPHER_MAC_SIZE = 2
PUBLIC_KEY_SIZE = 32


def _pad_zero(data: bytes) -> bytes:
    remainder = len(data) % CIPHER_BLOCK_SIZE
    if remainder == 0:
        return data
    return data + (b"\x00" * (CIPHER_BLOCK_SIZE - remainder))


def encrypt(shared_secret: bytes, plaintext: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(shared_secret[:CIPHER_KEY_SIZE]), modes.ECB())
    encryptor = cipher.encryptor()
    padded = _pad_zero(plaintext)
    return encryptor.update(padded) + encryptor.finalize()


def decrypt(shared_secret: bytes, ciphertext: bytes) -> bytes:
    if len(ciphertext) % CIPHER_BLOCK_SIZE != 0:
        raise ValueError("ciphertext length must be a multiple of 16")
    cipher = Cipher(algorithms.AES(shared_secret[:CIPHER_KEY_SIZE]), modes.ECB())
    decryptor = cipher.decryptor()
    return decryptor.update(ciphertext) + decryptor.finalize()


def encrypt_then_mac(shared_secret: bytes, plaintext: bytes) -> bytes:
    ciphertext = encrypt(shared_secret, plaintext)
    mac = hmac.new(shared_secret[:PUBLIC_KEY_SIZE], ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    return mac + ciphertext


def mac_then_decrypt(shared_secret: bytes, ciphertext_with_mac: bytes) -> bytes:
    if len(ciphertext_with_mac) <= CIPHER_MAC_SIZE:
        raise ValueError("ciphertext_with_mac too short")
    received_mac = ciphertext_with_mac[:CIPHER_MAC_SIZE]
    ciphertext = ciphertext_with_mac[CIPHER_MAC_SIZE:]
    expected_mac = hmac.new(shared_secret[:PUBLIC_KEY_SIZE], ciphertext, hashlib.sha256).digest()[:CIPHER_MAC_SIZE]
    if not hmac.compare_digest(received_mac, expected_mac):
        raise ValueError("message MAC mismatch")
    return decrypt(shared_secret, ciphertext)
