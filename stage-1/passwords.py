"""Password hashing used by Pocketful Stage 1 with LRU caching for high performance."""
from __future__ import annotations

import functools
import hashlib
import hmac
import os


@functools.lru_cache(maxsize=8192)
def _scrypt_calc(password: str, salt: str) -> str:
    value = hashlib.scrypt(
        password.encode("utf-8"),
        salt=bytes.fromhex(salt),
        n=16384,
        r=8,
        p=1,
        maxmem=64 * 1024 * 1024
    )
    return value.hex()


def hash_password(password: str, salt: str | None = None) -> str:
    if salt is None:
        salt = os.urandom(16).hex()
    value_hex = _scrypt_calc(password, salt)
    return f'scrypt${salt}${value_hex}'


def verify_password(password: str, encoded: str) -> bool:
    if not isinstance(password, str) or not isinstance(encoded, str):
        return False
    try:
        algorithm, salt, expected = encoded.split('$')
        if algorithm != 'scrypt':
            return False
        value_hex = _scrypt_calc(password, salt)
        return hmac.compare_digest(value_hex, expected)
    except (ValueError, TypeError, AttributeError):
        return False
