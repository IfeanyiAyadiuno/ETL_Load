"""Simple XOR obfuscation for application startup password check."""

from __future__ import annotations

_XOR_KEY = b"PCE_AppKey"
_EXPECTED_XOR_HEX = "6170726c714042"


def xor_bytes(data: bytes, key: bytes = _XOR_KEY) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def verify_password(text: str) -> bool:
    """Return True when ``text`` matches the configured application password."""
    if text is None:
        return False
    try:
        candidate = xor_bytes(str(text).encode("utf-8"))
    except Exception:
        return False
    return candidate.hex() == _EXPECTED_XOR_HEX
