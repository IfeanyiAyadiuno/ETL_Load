import configparser
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from whitson_credentials import (
    WhitsonCredentialsError,
    _load_credentials_from_env,
    load_whitson_credentials,
)


class TestWhitsonCredentials(unittest.TestCase):
    def test_load_credentials_from_env(self):
        with patch.dict(
            os.environ,
            {
                "WHITSON_CLIENT": "env-client",
                "WHITSON_CLIENT_ID": "env-id",
                "WHITSON_CLIENT_SECRET": "env-secret",
                "WHITSON_PROJECT_ID": "3",
            },
            clear=False,
        ):
            creds = _load_credentials_from_env()
        self.assertEqual(creds, ("env-client", "env-id", "env-secret", 3))

    def test_load_credentials_from_settings_ini(self):
        with tempfile.TemporaryDirectory() as tmp:
            ini = Path(tmp) / "settings.ini"
            cfg = configparser.ConfigParser()
            cfg["WHITSON"] = {
                "client": "test-client",
                "client_id": "id-123",
                "client_secret": "secret-456",
                "project_id": "7",
            }
            with ini.open("w", encoding="utf-8") as fh:
                cfg.write(fh)

            with patch("whitson_credentials.get_settings_path", return_value=str(ini)):
                creds = load_whitson_credentials()

        self.assertEqual(creds, ("test-client", "id-123", "secret-456", 7))

    def test_settings_ini_takes_precedence_over_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            ini = Path(tmp) / "settings.ini"
            cfg = configparser.ConfigParser()
            cfg["WHITSON"] = {
                "client": "ini-client",
                "client_id": "ini-id",
                "client_secret": "ini-secret",
                "project_id": "2",
            }
            with ini.open("w", encoding="utf-8") as fh:
                cfg.write(fh)

            with patch("whitson_credentials.get_settings_path", return_value=str(ini)):
                with patch.dict(
                    os.environ,
                    {
                        "WHITSON_CLIENT": "env-client",
                        "WHITSON_CLIENT_ID": "env-id",
                        "WHITSON_CLIENT_SECRET": "env-secret",
                        "WHITSON_PROJECT_ID": "9",
                    },
                    clear=False,
                ):
                    creds = load_whitson_credentials()

        self.assertEqual(creds[0], "ini-client")

    def test_missing_credentials_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            ini = Path(tmp) / "settings.ini"
            ini.write_text("[SQL]\nserver = x\n", encoding="utf-8")
            with patch("whitson_credentials.get_settings_path", return_value=str(ini)):
                with patch.dict(os.environ, {}, clear=True):
                    with self.assertRaises(WhitsonCredentialsError):
                        load_whitson_credentials()


if __name__ == "__main__":
    unittest.main()
