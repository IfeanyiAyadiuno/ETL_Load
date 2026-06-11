"""
Configure HTTPS trust for requests / http.client in corporate Windows environments.

Frozen PyInstaller builds use certifi by default, which does not include corporate
proxy root CAs installed in the Windows certificate store. On Windows we prefer the
OS trust store unless a custom CA bundle is configured.
"""

from __future__ import annotations

import configparser
import os
import sys
from pathlib import Path

_CONFIGURED = False


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _settings_ca_bundle() -> str | None:
    try:
        from app_paths import get_settings_path
    except ImportError:
        return None

    path = get_settings_path()
    if not path or not os.path.isfile(path):
        return None

    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    if not cfg.has_section("WHITSON"):
        return None

    bundle = cfg.get("WHITSON", "ca_bundle", fallback="").strip()
    if bundle and os.path.isfile(bundle):
        return bundle
    if bundle:
        print(
            f"[ssl] WHITSON ca_bundle not found: {bundle} "
            "(check settings.ini [WHITSON] ca_bundle path)"
        )
    return None


def resolve_ca_bundle() -> str | None:
    """Return an explicit CA bundle path if configured, else None."""
    for key in ("WHITSON_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        raw = os.environ.get(key, "").strip()
        if raw and os.path.isfile(raw):
            return raw

    return _settings_ca_bundle()


def configure_ssl_trust() -> None:
    """
    Apply SSL trust configuration once per process.

    Priority:
    1. WHITSON_CA_BUNDLE / REQUESTS_CA_BUNDLE / SSL_CERT_FILE env vars
    2. settings.ini [WHITSON] ca_bundle
    3. Windows OS certificate store (truststore) when not disabled
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    if _truthy(os.environ.get("WHITSON_SSL_USE_CERTIFI_ONLY")):
        return

    bundle = resolve_ca_bundle()
    if bundle:
        os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle)
        os.environ.setdefault("SSL_CERT_FILE", bundle)
        return

    if sys.platform == "win32":
        try:
            import truststore

            truststore.inject_into_ssl()
        except ImportError:
            pass
