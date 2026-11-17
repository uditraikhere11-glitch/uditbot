import json
import os
from pathlib import Path
import tempfile
import unittest

from src.engine.tracker_credentials import CredentialEvidenceError, fingerprint


class CredentialFingerprintTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = self.tmp.name

    def test_stable_across_alias_case(self):
        a = fingerprint({"DHAN_PIN": "1234", "DHAN_TOTP_SECRET": "AAAA"}, self.dir)
        b = fingerprint({"dhan_pin": "1234", "dhan_totp_secret": "AAAA"}, self.dir)
        self.assertEqual(a, b)
        self.assertEqual(64, len(a))
        self.assertNotIn("1234", a)

    def test_changed_pin_changes_fingerprint(self):
        a = fingerprint({"DHAN_PIN": "1234"}, self.dir)
        b = fingerprint({"DHAN_PIN": "4321"}, self.dir)
        self.assertNotEqual(a, b)

    def test_private_key_mode(self):
        fingerprint({"DHAN_PIN": "1234"}, self.dir)
        key = Path(self.dir) / ".tracker_credential_hmac_key"
        self.assertEqual(0o600, key.stat().st_mode & 0o777)
        self.assertEqual(32, len(key.read_bytes()))

    def test_short_key_blocks(self):
        Path(self.dir, ".tracker_credential_hmac_key").write_bytes(b"bad")
        with self.assertRaises(CredentialEvidenceError):
            fingerprint({"DHAN_PIN": "1234"}, self.dir)

    def test_key_symlink_blocks(self):
        path = Path(self.dir, ".tracker_credential_hmac_key")
        other = Path(self.dir, "outside")
        other.write_bytes(b"x" * 32)
        path.symlink_to(other)
        with self.assertRaises(CredentialEvidenceError):
            fingerprint({"DHAN_PIN": "1234"}, self.dir)

    def test_no_plaintext_secrets_on_disk(self):
        fingerprint({"DHAN_PIN": "1234", "DHAN_TOTP_SECRET": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}, self.dir)
        disk = b"".join(f.read_bytes() for f in Path(self.dir).iterdir() if f.is_file())
        self.assertNotIn(b"1234", disk)
        self.assertNotIn(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ", disk)


if __name__ == "__main__":
    unittest.main()
