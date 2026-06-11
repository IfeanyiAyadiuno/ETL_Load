import configparser
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from whitson_credentials import (
    WhitsonCredentialsError,
    _load_credentials_from_script,
    load_whitson_credentials,
)


class TestWhitsonCredentials(unittest.TestCase):
    def test_load_credentials_from_script(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "whitson_upload.py"
        client, client_id, client_secret, project_id = _load_credentials_from_script(script)
        self.assertTrue(client)
        self.assertTrue(client_id)
        self.assertTrue(client_secret)
        self.assertGreaterEqual(project_id, 1)

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

    def test_missing_credentials_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            ini = Path(tmp) / "settings.ini"
            ini.write_text("[SQL]\nserver = x\n", encoding="utf-8")
            with patch("whitson_credentials.get_settings_path", return_value=str(ini)):
                with patch("whitson_credentials._resolve_whitson_upload_script_path", return_value=None):
                    with self.assertRaises(WhitsonCredentialsError):
                        load_whitson_credentials()


if __name__ == "__main__":
    unittest.main()
