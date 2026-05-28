"""Application directory and settings.ini path (frozen exe vs source)."""

import sys
from pathlib import Path


def _app_dir():
    """Return the application directory, whether running from source or frozen exe."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def get_settings_path():
    """Absolute path to settings.ini (next to the exe when frozen, else next to this file)."""
    return str(_app_dir() / "settings.ini")


def get_logo_path():
    """Absolute path to the main-window logo (images/pce_logo.png)."""
    return str(_app_dir() / "images" / "pce_logo.png")


def get_company_icon_path():
    """Absolute path to the header company icon (images/PCE Icon white.png)."""
    return str(_app_dir() / "images" / "PCE Icon white.png")
