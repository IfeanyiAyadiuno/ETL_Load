import os
import tempfile
import unittest
from unittest.mock import patch

import ssl_trust


class TestSslTrust(unittest.TestCase):
    def setUp(self):
        ssl_trust._CONFIGURED = False

    def test_resolve_ca_bundle_from_env(self):
        with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as fh:
            fh.write("dummy")
            path = fh.name
        try:
            with patch.dict(os.environ, {"WHITSON_CA_BUNDLE": path}, clear=False):
                self.assertEqual(ssl_trust.resolve_ca_bundle(), path)
        finally:
            os.unlink(path)

    def test_configure_uses_explicit_bundle(self):
        with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as fh:
            fh.write("dummy")
            path = fh.name
        try:
            with patch.dict(
                os.environ,
                {"WHITSON_CA_BUNDLE": path, "WHITSON_SSL_USE_CERTIFI_ONLY": ""},
                clear=False,
            ):
                ssl_trust.configure_ssl_trust()
                self.assertEqual(os.environ.get("REQUESTS_CA_BUNDLE"), path)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
