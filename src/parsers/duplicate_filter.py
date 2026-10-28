"""
Cross-group deduplication engine with 20-second sliding window per underlying stock symbol.
Guarantees deterministic clock semantics and cross-group suppression.
"""

import time
from typing import Callable, Dict, Optional, Tuple


class DuplicateFilter:
    """
    Tracks incoming stock signals across all configured Telegram groups.

    Rules:
      1. Duplicate Key: Normalized stock underlying symbol (e.g. 'TATAMOTORS', 'SBIN').
      2. Sliding Window: 20.0 seconds.
      3. Cross-group: A signal in Group A blocks duplicate signals for the same stock
         arriving from Group B within 20s.
      4. Different price, strike, or expiry for the same stock are considered DUPLICATES
         within the 20s window.
      5. Re-entry outside the 20s window is explicitly permitted.
    """

    def __init__(self, window_sec: float = 20.0, clock: Optional[Callable[[], float]] = None):
        self.window_sec = float(window_sec)
        self.clock = clock or time.time
        # Map: stock_symbol -> (last_seen_epoch_sec, channel_id, message_id)
        self._seen: Dict[str, Tuple[float, int, int]] = {}

    def is_duplicate(
        self,
        stock_symbol: str,
        channel_id: int = 0,
        message_id: int = 0,
        event_time: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """
        Checks if the stock symbol has been processed within the 20-second sliding window.

        Returns:
            (is_dup, reason)
            If False (not duplicate), updates the internal state with current timestamp.
        """
        if not stock_symbol:
            return True, "Empty stock symbol"

        sym = stock_symbol.strip().upper()
        now = event_time if event_time is not None else self.clock()

        if sym in self._seen:
            last_time, prev_channel, prev_msg = self._seen[sym]
            elapsed = now - last_time

            if elapsed <= self.window_sec:
                # Still within the 20-second deduplication window
                reason = (
                    f"Duplicate signal for {sym} within {elapsed:.1f}s window "
                    f"(prev channel={prev_channel}, msg={prev_msg})"
                )
                return True, reason

        # Not duplicate or window expired -> record new entry and allow
        self._seen[sym] = (now, channel_id, message_id)
        return False, f"Fresh signal for {sym} (window clear)"

    def clear(self) -> None:
        """Clears all deduplication history."""
        self._seen.clear()

    def prune(self, max_retention_sec: float = 300.0) -> None:
        """Removes entries older than retention window to keep memory bounded."""
        now = self.clock()
        expired = [sym for sym, (t, _, _) in self._seen.items() if (now - t) > max_retention_sec]
        for sym in expired:
            del self._seen[sym]
