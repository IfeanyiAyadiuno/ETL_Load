"""
Resolve Whitson+ API credentials for GUI and push code.

Order: ``settings.ini`` ``[WHITSON]``, then ``WHITSON_*`` environment variables.
"""

from __future__ import annotations

import configparser
import os
from typing import Tuple

from app_paths import get_settings_path


class WhitsonCredentialsError(Exception):
    """Whitson+ client credentials are missing or incomplete."""


def _load_credentials_from_env() -> Tuple[str, str, str, int] | None:
    client = os.getenv("WHITSON_CLIENT", "").strip()
    client_id = os.getenv("WHITSON_CLIENT_ID", "").strip()
    client_secret = os.getenv("WHITSON_CLIENT_SECRET", "").strip()
    project_raw = os.getenv("WHITSON_PROJECT_ID", "").strip()
    if not (client and client_id and client_secret and project_raw):
        return None
    try:
        project_id = int(project_raw)
    except ValueError as exc:
        raise WhitsonCredentialsError(
            f"WHITSON_PROJECT_ID must be an integer, got {project_raw!r}."
        ) from exc
    if project_id < 1:
        raise WhitsonCredentialsError(
            f"WHITSON_PROJECT_ID must be positive, got {project_id}."
        )
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

    Order: settings.ini [WHITSON], then WHITSON_* environment variables.
    """
    from_ini = _load_credentials_from_settings_ini()
    if from_ini is not None:
        return from_ini

    from_env = _load_credentials_from_env()
    if from_env is not None:
        return from_env

    raise WhitsonCredentialsError(
        "Whitson+ credentials not found. Add a [WHITSON] section to settings.ini "
        "(next to the application exe) with client, client_id, client_secret, and "
        "project_id, or set WHITSON_CLIENT, WHITSON_CLIENT_ID, WHITSON_CLIENT_SECRET, "
        "and WHITSON_PROJECT_ID environment variables."
    )


def get_default_project_id() -> int:
    """Default Whitson+ project_id from settings.ini or environment."""
    return load_whitson_credentials()[3]


def get_whitson_project_source() -> Tuple[int, str]:
    """Return (default_project_id, short source label for UI)."""
    try:
        return get_default_project_id(), "settings.ini [WHITSON]"
    except WhitsonCredentialsError:
        return 2, "built-in fallback — add [WHITSON] to settings.ini"


def _optional_project_label_from_ini() -> str:
    path = get_settings_path()
    if not path or not os.path.isfile(path):
        return ""
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(path, encoding="utf-8")
    if not cfg.has_section("WHITSON"):
        return ""
    return cfg.get("WHITSON", "project_label", fallback="").strip()


def format_whitson_project_hint(*, selected_id: int | None = None) -> str:
    """
    Human-readable hint for the Whitson+ project picker.

    Optional settings.ini ``[WHITSON] project_label`` adds a friendly name
    (e.g. ``Montney base case``).
    """
    default_id, source = get_whitson_project_source()
    label = _optional_project_label_from_ini()
    purpose = (
        "Whitson+ project where PCE daily production is posted and well "
        "attributes are synced"
    )
    if label:
        purpose = f"{label} — {purpose}"

    active_id = default_id if selected_id is None else selected_id
    if selected_id is not None and selected_id != default_id:
        return (
            f"{purpose} · using project ID {active_id} "
            f"(default {default_id} from {source})"
        )
    return f"{purpose} · default project ID {default_id} ({source})"
