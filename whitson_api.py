"""
Thin Whitson+ API facade for the ETL application.

Delegates to ``whitson_connect.WhitsonConnection`` while credentials are resolved
via ``whitson_credentials``. A future pass can move the six used methods here
and archive the legacy monolith.
"""

from __future__ import annotations

from typing import Tuple

from whitson_connect import WhitsonConnection
from whitson_credentials import load_whitson_credentials


def create_whitson_connection() -> Tuple[WhitsonConnection, int]:
    """Return (connection, project_id) using settings.ini or environment."""
    client, client_id, client_secret, project_id = load_whitson_credentials()
    return WhitsonConnection(client, client_id, client_secret), project_id


__all__ = ["WhitsonConnection", "create_whitson_connection", "load_whitson_credentials"]
