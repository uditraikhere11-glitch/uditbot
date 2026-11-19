"""CSS-first finishing pass; verify original Setup inputs remain authoritative."""
from __future__ import annotations

from pathlib import Path
import ast
import hashlib
import unittest

ROOT = Path(__file__).resolve().parents[1]
PRE_REPLICA_SETUP_GIT_BLOB = 'c8a81e63d3045229c97cbe7e5f58085b2395e6bf'


def git_blob_sha(raw: bytes) -> str:
    return hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\x00' + raw).hexdigest()


class PremiumFinishContract(unittest.TestCase):
    def test_exact_working_setup_source_not_changed(self):
        raw = (ROOT / 'src/setup_app.py').read_bytes()
        self.assertEqual(git_blob_sha(raw), PRE_REPLICA_SETUP_GIT_BLOB)
        src = raw.decode()
        ast.parse(src)
        self.assertNotIn('replicaBoard', src)
        self.assertIn("fetch('/setup/save-apply'", src)

    def test_visual_assets_do_not_modify_form_controls_or_broker_state(self):
        css = (ROOT / 'src/premium_finish.css').read_text()
        js = (ROOT / 'src/premium_layout.js').read_text()
        self.assertIn('prem-balanced-columns', css)
        self.assertIn('column-count:2', css)
        self.assertIn('@media(max-width:1279px)', css)
        self.assertIn('@media(max-width:760px)', css)
        for forbidden in ('fetch(', 'XMLHttpRequest', 'FormData', '.value =',
                          '.checked =', 'requestSubmit(', 'submit(',
                          'localStorage', 'sessionStorage', 'window.confirm('):
            self.assertNotIn(forbidden, js)
        for id_ in ('setup-execution', 'setup-telegram', 'setup-broker',
                    'setup-sizing', 'setup-risk', 'setup-protection'):
            self.assertIn(id_, js)
        self.assertIn("window.addEventListener('resize'", js)

    def test_shared_visual_assets_embedded_once(self):
        from src.premium_ui import premium_styles, premium_nav_js, premium_nav
        css = premium_styles()
        js = premium_nav_js()
        self.assertIn('prem-balanced-columns', css)
        self.assertEqual(css.count('PR #107 finishing pass'), 1)
        self.assertIn('prem-balanced-columns', js)
        self.assertIn('premNavOpen', js)
        self.assertEqual(premium_nav('setup').count('<a href='), 3)
        self.assertNotIn('data-mirror', js)

    def test_no_duplicate_controls_or_unconfigured_routes(self):
        for path in ('src/premium_layout.js', 'src/premium_finish.css'):
            text = (ROOT / path).read_text()
            self.assertNotIn('/control/amo/fno', text)
            self.assertNotIn('/logs', text)
            self.assertNotIn('/reports', text)
        src = (ROOT / 'src/premium_ui.py').read_text()
        self.assertNotIn('approved_replica_styles', src)
        self.assertNotIn('approved_replica_js', src)


if __name__ == '__main__':
    unittest.main()
