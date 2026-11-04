"""
Durable Outbox Worker for Asynchronous Log & Signal Notifications.
Implements STEP 16 (16-A through 16-E) + V12-14 Non-Blocking I/O:
- Separates broker order execution from Telegram network latency.
- Offloads synchronous SQLite and HTTP dispatch work to `asyncio.to_thread` in `run()` so the
  event loop is never blocked.
- Bounded retries with exponential backoff and dead-letter escalation on repeated 429/500/timeout (`OUTBOX-01`).
- Redacts sensitive credentials (tokens, PINs, TOTP secrets, bot URLs, JWTs) using a callable replacement.
"""

import asyncio
import json
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

_KV_SECRET_RE = re.compile(
    r'("?\b(?:access_token|dhan_access_token|token|pin|dhan_pin|totp_secret|dhan_totp_secret|api_hash|tg_api_hash|api_key|secret)"?\s*[:=]\s*"?)([^",\s\}]+)("?)',
    re.IGNORECASE,
)
_JWT_RE = re.compile(r"\beyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\b")
_BOT_URL_RE = re.compile(r"bot\d{6,15}:[A-Za-z0-9_-]{20,}")


def redact_secrets(text: str) -> str:
    """
    16-E: Masks authentication tokens, Dhan PIN, API credentials, Telegram bot tokens,
    and JWTs from log payloads using callable replacements (zero group reference risk).
    """
    if not text or not isinstance(text, str):
        return ""

    redacted = _KV_SECRET_RE.sub(lambda m: f"{m.group(1)}***REDACTED***{m.group(3)}", text)
    redacted = _JWT_RE.sub(lambda _: "***REDACTED_JWT***", redacted)
    redacted = _BOT_URL_RE.sub(lambda _: "bot***REDACTED_BOT_TOKEN***", redacted)
    return redacted


class OutboxWorker:
    def __init__(
        self,
        journal_db: Any,
        dispatch_fn: Callable[[str], bool],
        poll_interval_sec: float = 1.0,
        max_retries: int = 5,
    ):
        self.db = journal_db
        self.dispatch_fn = dispatch_fn
        self.poll_interval_sec = poll_interval_sec
        self.max_retries = max_retries
        self.running: bool = False
        self._lock = threading.Lock()

    def process_pending_events(self, limit: int = 10, now: Optional[float] = None) -> int:
        """
        Fetches pending outbox events from SQLite whose `next_retry_at <= now`,
        redacts sensitive tokens, and dispatches to destination.
        Marks processed atomically upon success, or schedules exponential backoff on failure.
        """
        with self._lock:
            current_ts = now if now is not None else time.time()
            try:
                pending = self.db.fetch_pending_outbox(limit=limit, now=current_ts)
            except TypeError:
                pending = self.db.fetch_pending_outbox(limit=limit)

            if not pending:
                return 0

            dispatched_count = 0
            for item in pending:
                out_id = item["id"]
                payload_raw = item["payload"]
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                    msg_text = payload.get("text") or payload.get("message") or str(payload)
                    safe_text = redact_secrets(msg_text)

                    ok = self.dispatch_fn(safe_text)
                    if ok:
                        self.db.mark_outbox_processed(out_id)
                        dispatched_count += 1
                    else:
                        logger.warning(f"[Outbox] Delivery returned False for outbox item {out_id}; scheduling retry.")
                        if hasattr(self.db, "mark_outbox_retry"):
                            self.db.mark_outbox_retry(
                                out_id,
                                error_msg="Dispatch returned False (unconfigured notifier / rate limit / network failure)",
                                max_retries=self.max_retries,
                                now=current_ts,
                            )
                except Exception as e:
                    safe_err = redact_secrets(str(e))
                    logger.error(f"[Outbox] Exception processing outbox item {out_id}: {safe_err}")
                    if hasattr(self.db, "mark_outbox_retry"):
                        self.db.mark_outbox_retry(
                            out_id,
                            error_msg=safe_err,
                            max_retries=self.max_retries,
                            now=current_ts,
                        )

            return dispatched_count

    async def run(self):
        """Asynchronous non-blocking worker loop (`V12-14`)."""
        self.running = True
        logger.info("📬 [OUTBOX] Durable asynchronous outbox worker started.")
        while self.running:
            try:
                await asyncio.to_thread(self.process_pending_events, 10)
            except Exception as e:
                logger.error(f"[OUTBOX] Loop error: {redact_secrets(str(e))}")
            await asyncio.sleep(self.poll_interval_sec)

    def stop(self):
        self.running = False
