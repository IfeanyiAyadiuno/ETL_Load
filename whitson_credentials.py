"""
Resolve Whitson+ API credentials for GUI and push code.

Frozen exe builds do not ship ``scripts/whitson_upload.py`` next to the bundle.
Credentials are read from ``settings.ini`` ``[WHITSON]`` when present, otherwise
from ``scripts/whitson_upload.py`` (repo dev or ``<app_dir>/scripts/``).
"""

from __future__ import annotations

import configparser
import importlib.util
import os
from pathlib import Path
from typing import Tuple

from app_paths import _app_dir, get_settings_path

_REPO_ROOT = Path(__file__).resolve().parent


class WhitsonCredentialsError(Exception):
    """Whitson+ client credentials are missing or incomplete."""


def _resolve_whitson_upload_script_path() -> Path | None:
    candidates = (
        _app_dir() / "scripts" / "whitson_upload.py",
        _REPO_ROOT / "scripts" / "whitson_upload.py",
    )
    for path in candidates:
        if path.is_file():
            return path
    return None


def _load_credentials_from_script(script: Path) -> Tuple[str, str, str, int]:
    spec = importlib.util.spec_from_file_location("whitson_upload_credentials", script)
    if spec is None or spec.loader is None:
        raise WhitsonCredentialsError(f"Cannot load Whitson credentials from {script}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        client = str(mod.CLIENT).strip()
        client_id = str(mod.CLIENT_ID).strip()
        client_secret = str(mod.CLIENT_SECRET).strip()
        project_id = int(mod.PROJECT_ID)
    except AttributeError as exc:
        raise WhitsonCredentialsError(
            f"{script} must define CLIENT, CLIENT_ID, CLIENT_SECRET, and PROJECT_ID."
        ) from exc
    if not (client and client_id and client_secret):
        raise WhitsonCredentialsError(f"Whitson credentials in {script} are incomplete.")
    if project_id < 1:
        raise WhitsonCredentialsError(f"Invalid PROJECT_ID in {script}: {project_id}")
    return client, client_id, client_secret, project_id


def _load_credentials_from_settings_ini() -> Tuple[str, str, str, int] | None:
    path = get_settings_path()
    if not path or not os.path.isfile(path):
        return None
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    if not cfg.has_section("WHITSON"):
        return None

    client = cfg.get("WHITSON", "client", fallback="").strip()
    client_id = cfg.get("WHITSON", "client_id", fallback="").strip()
    client_secret = cfg.get("WHITSON", "client_secret", fallback="").strip()
    project_raw = cfg.get("WHITSON", "project_id", fallback="").strip()
    if not (client and client_id and client_secret and project_raw):
        return None
    try:
        project_id = int(project_raw)
    except ValueError as exc:
        raise WhitsonCredentialsError(
            f"settings.ini [WHITSON] project_id must be an integer, got {project_raw!r}."
        ) from exc
    if project_id < 1:
        raise WhitsonCredentialsError(
            f"settings.ini [WHITSON] project_id must be positive, got {project_id}."
        )
    return client, client_id, client_secret, project_id


def load_whitson_credentials() -> Tuple[str, str, str, int]:
    """
    Return (client, client_id, client_secret, project_id).

    Order: settings.ini [WHITSON], then scripts/whitson_upload.py.
    """
    from_ini = _load_credentials_from_settings_ini()
    if from_ini is not None:
        return from_ini

    script = _resolve_whitson_upload_script_path()
    if script is not None:
        return _load_credentials_from_script(script)

    raise WhitsonCredentialsError(
        "Whitson+ credentials not found. Add a [WHITSON] section to settings.ini "
        "(next to the application exe) with client, client_id, client_secret, and "
        "project_id, or place scripts/whitson_upload.py beside the exe."
    )


def get_default_project_id() -> int:
    """Default Whitson+ project_id from settings.ini or scripts/whitson_upload.py."""
    return load_whitson_credentials()[3]
