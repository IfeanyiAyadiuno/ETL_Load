"""Tests for application XOR password check."""

import unittest

from app_password import verify_password, xor_bytes


class TestAppPassword(unittest.TestCase):
    def test_verify_correct_password(self):
        self.assertTrue(verify_password("BeerNow"))

    def test_verify_wrong_password(self):
        self.assertFalse(verify_password("wrong"))
        self.assertFalse(verify_password(""))
        self.assertFalse(verify_password("1373002"))

    def test_xor_roundtrip(self):
        plain = b"1373002"
        self.assertEqual(xor_bytes(xor_bytes(plain)), plain)


if __name__ == "__main__":
    unittest.main()
