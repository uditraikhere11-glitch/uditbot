"""
Telegram Log Group Notifier.
Dispatches formatted trade execution cards to Udit's configured Telegram Log Group.
Implements V12-18 & V12-14 fixes:
- Reads `tg_bot_token` configured via `AppConfig` (`TG_BOT_TOKEN`).
- Distinguishes `DELIVERED`, `LOCAL_ONLY_DISABLED`, and `FAILED` statuses; returns `False` when
  disabled so `OutboxWorker` never marks undelivered critical alerts as `PROCESSED`.
- Uses bounded 5-second HTTP timeout.
"""

import logging
import time
import math
from src.notifier.outbox_worker import TelegramRetryAfter, redact_secrets
import requests
from src.config import AppConfig

logger = logging.getLogger("telegram_notifier")


class TelegramNotifier:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.bot_token = (getattr(config, "tg_bot_token", "") or "").strip()
        self.log_group_id = config.log_group_id
        self.retry_at = 0.0
        self.enabled = bool(self.bot_token and self.log_group_id)
        self.last_delivery_status: str = "ENABLED" if self.enabled else "DISABLED"

    def send_log_message(self, formatted_text: str, *, outbox: bool = False) -> bool:
        """
        Sends formatted text to configured Telegram Log Group.
        Returns True ONLY when actually delivered to Telegram (never when disabled).
        """
        formatted_text = redact_secrets(formatted_text)
        logger.info("LOG GROUP NOTIFICATION:\n%s", formatted_text)

        if not self.enabled:
            self.last_delivery_status = "LOCAL_ONLY_DISABLED"
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.log_group_id,
            "text": formatted_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            delay = self.retry_at - time.monotonic()
            if delay > 0:
                raise TelegramRetryAfter(math.ceil(delay))
            for attempt in range(2):
                resp = requests.post(url, json=payload, timeout=5)
                try:
                    body = resp.json()
                except ValueError:
                    body = {}
                if not isinstance(body, dict):
                    body = {}
                if resp.status_code == 200 and body.get("ok") is True:
                    self.last_delivery_status = "DELIVERED"
                    return True
                self.last_delivery_status = f"HTTP_{resp.status_code}"
                if resp.status_code == 429:
                    seconds = (body.get("parameters") or {}).get("retry_after", 1)
                    if type(seconds) is not int or seconds < 1:
                        seconds = 1
                    self.retry_at = time.monotonic() + seconds
                    raise TelegramRetryAfter(seconds)
                if attempt == 0 and resp.status_code == 400 and "parse entities" in str(body.get("description", "")).lower():
                    payload = {k: v for k, v in payload.items() if k != "parse_mode"}
                    continue
                return False
        except TelegramRetryAfter:
            self.last_delivery_status = "RATE_LIMITED"
            if outbox:
                raise
        except Exception as exc:
            self.last_delivery_status = f"ERROR: {type(exc).__name__}"
            logger.warning("Telegram log delivery failed (%s)", type(exc).__name__)
        return False
