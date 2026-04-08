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
