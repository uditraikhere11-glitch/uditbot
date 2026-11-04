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
import requests
from src.config import AppConfig

logger = logging.getLogger("telegram_notifier")


class TelegramNotifier:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.bot_token = (getattr(config, "tg_bot_token", "") or "").strip()
        self.log_group_id = config.log_group_id
        self.enabled = bool(self.bot_token and self.log_group_id)
        self.last_delivery_status: str = "ENABLED" if self.enabled else "DISABLED"

    def send_log_message(self, formatted_text: str) -> bool:
        """
        Sends formatted text to configured Telegram Log Group.
        Returns True ONLY when actually delivered to Telegram (never when disabled).
        """
        logger.info(f"LOG GROUP NOTIFICATION:\n{formatted_text}")

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
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                self.last_delivery_status = "DELIVERED"
                return True
            logger.warning(f"Failed to post to Telegram Log Group ({resp.status_code}): {resp.text}")
            # Retry once without HTML parse_mode in case of entity parsing issues
            payload.pop("parse_mode", None)
            retry_resp = requests.post(url, json=payload, timeout=5)
            ok = retry_resp.status_code == 200
            self.last_delivery_status = "DELIVERED" if ok else f"HTTP_{retry_resp.status_code}"
            return ok
        except Exception as e:
            self.last_delivery_status = f"ERROR: {e}"
            logger.error(f"Network exception posting to Telegram Log Group: {e}")
            return False
