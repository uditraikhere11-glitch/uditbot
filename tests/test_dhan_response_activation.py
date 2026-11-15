"""AC-12: verified Telegram routing, preserved previous destination and no false ack.

Pure mocks: no Telegram connection, Dhan API, service restart or production files.
"""
from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from src.notifier.response_activation import (
    stage_destination_change, resolve_with_fallback, read_active_destination,
)

OLD = '-100550011111'
NEW = '-5514889030'


class DestinationActivationAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.root = self.t.name

    async def test_ac12_verified_target_activates_without_broker_or_fallback(self):
        stage_destination_change(self.root, OLD, NEW)
        resolver = AsyncMock(return_value=object())
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', resolver):
            entity, active, verified = await resolve_with_fallback(None, NEW, [-10012345], self.root)
        self.assertIsNotNone(entity)
        self.assertTrue(verified)
        self.assertEqual(active, NEW)
        resolver.assert_awaited_once()
        ack = read_active_destination(self.root)
        self.assertEqual(ack['requested_name'], NEW)
        self.assertTrue(ack['target_verified'])
        self.assertFalse(ack['fallback_active'])
        self.assertEqual(Path(self.root, 'dhan_response_activation.json').stat().st_mode & 0o077, 0)

    async def test_ac12_failed_target_reuses_only_validated_previous_destination(self):
        stage_destination_change(self.root, OLD, NEW)
        sentinel=object()
        async def resolve(_client, name, _forbidden):
            if name == NEW:
                raise ValueError('New destination not a joined unique group')
            if name == OLD:
                return sentinel
            raise AssertionError('Unexpected destination')
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', side_effect=resolve) as resolver:
            entity, active, verified = await resolve_with_fallback(None, NEW, [-10012345], self.root)
        self.assertIs(entity, sentinel)
        self.assertEqual(active, OLD)
        self.assertFalse(verified)
        self.assertEqual(resolver.await_count, 2)
        ack = read_active_destination(self.root)
        self.assertTrue(ack['fallback_active'])
        self.assertFalse(ack['target_verified'])
        self.assertEqual(ack['active_name'], OLD)

    async def test_ac12_disconnect_does_not_mark_requested_or_fallback_verified(self):
        stage_destination_change(self.root, OLD, NEW)
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', side_effect=OSError('offline')) as resolver:
            entity, active, verified = await resolve_with_fallback(None, NEW, [], self.root)
        self.assertIsNone(entity)
        self.assertEqual(active, '')
        self.assertFalse(verified)
        self.assertEqual(resolver.await_count, 2)
        self.assertFalse(read_active_destination(self.root)['target_verified'])

    async def test_ac12_no_fallback_to_unverified_name_or_signal_source(self):
        stage_destination_change(self.root, OLD, NEW)
        async def resolve(_client, name, _forbidden):
            raise ValueError('Unjoined or forbidden signal-source group')
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', side_effect=resolve):
            entity, active, verified = await resolve_with_fallback(None, NEW, [int(OLD)], self.root)
        self.assertEqual((entity, active, verified), (None, '', False))

    async def test_ac12_intentional_disable_never_reactivates_old_target(self):
        stage_destination_change(self.root, OLD, '')
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', side_effect=AssertionError('Disabled must not resolve')) as resolver:
            entity, active, verified = await resolve_with_fallback(None, '', [], self.root)
        self.assertEqual((entity, active, verified), (None, '', True))
        resolver.assert_not_called()

    async def test_ac12_multiple_pending_edits_preserve_initial_verified_candidate(self):
        stage_destination_change(self.root, OLD, '-100777888999')
        stage_destination_change(self.root, '-100777888999', NEW)
        seen = []
        async def resolve(_client, name, _forbidden):
            seen.append(name)
            if name == NEW:
                raise ValueError('not joined')
            return 'verified:' + name
        with patch('src.notifier.dhan_response.resolve_dhan_response_group', side_effect=resolve):
            entity, active, verified = await resolve_with_fallback(None, NEW, [], self.root)
        self.assertEqual(seen, [NEW, OLD])
        self.assertFalse(verified)
        self.assertEqual((entity, active), ('verified:'+OLD, OLD))


if __name__ == '__main__':
    unittest.main()
