"""
Unit tests for 20-second cross-group deduplication engine.
"""

import unittest
from src.parsers.duplicate_filter import DuplicateFilter


class TestDuplicateFilter(unittest.TestCase):
    def test_20s_cross_group_dedup(self):
        current_time = 1000.0
        clock = lambda: current_time
        dedup = DuplicateFilter(window_sec=20.0, clock=clock)

        # 1. First signal for TATAMOTORS in Group A at t=1000s -> Allowed
        is_dup, reason = dedup.is_duplicate("TATAMOTORS", channel_id=-1001, message_id=101)
        self.assertFalse(is_dup)
        self.assertIn("Fresh signal", reason)

        # 2. Second signal for TATAMOTORS in Group B at t=1010s (elapsed 10s <= 20s) -> Duplicate
        current_time = 1010.0
        is_dup, reason = dedup.is_duplicate("TATAMOTORS", channel_id=-1002, message_id=505)
        self.assertTrue(is_dup)
        self.assertIn("Duplicate signal for TATAMOTORS within 10.0s window", reason)

        # 3. Signal for a DIFFERENT stock (SBIN) at t=1015s -> Allowed immediately
        current_time = 1015.0
        is_dup, reason = dedup.is_duplicate("SBIN", channel_id=-1002, message_id=506)
        self.assertFalse(is_dup)
        self.assertIn("Fresh signal", reason)

        # 4. Re-entry for TATAMOTORS at t=1021s (elapsed 21s > 20s) -> Allowed
        current_time = 1021.0
        is_dup, reason = dedup.is_duplicate("TATAMOTORS", channel_id=-1001, message_id=102)
        self.assertFalse(is_dup)
        self.assertIn("Fresh signal for TATAMOTORS (window clear)", reason)

        # 5. Immediate duplicate after the re-entry at t=1025s (elapsed 4s) -> Duplicate
        current_time = 1025.0
        is_dup, reason = dedup.is_duplicate("TATAMOTORS", channel_id=-1003, message_id=999)
        self.assertTrue(is_dup)


if __name__ == "__main__":
    unittest.main()
