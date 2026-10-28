"""
Telegram Log Group Notifier.
Dispatches formatted trade execution cards to Udit's configured Telegram Log Group.
"""

import logging
from typing import Optional
import requests
from src.config import AppConfig

logger = logging.getLogger("telegram_notifier")


class TelegramNotifier:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.bot_token = getattr(config, "tg_bot_token", "")
        self.log_group_id = config.log_group_id
        self.enabled = bool(self.bot_token and self.log_group_id)

    def send_log_message(self, formatted_text: str) -> bool:
        """
        Sends formatted text to configured Telegram Log Group.
        """
        logger.info(f"LOG GROUP NOTIFICATION:\n{formatted_text}")

        if not self.enabled:
            return True  # Fallback to local logging when bot token/channel is unconfigured

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.log_group_id,
            "text": formatted_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return True
            logger.warning(f"Failed to post to Telegram Log Group ({resp.status_code}): {resp.text}")
            # Retry once without HTML parse_mode in case of entity parsing issues
            payload.pop("parse_mode", None)
            retry_resp = requests.post(url, json=payload, timeout=10)
            return retry_resp.status_code == 200
        except Exception as e:
            logger.error(f"Network exception posting to Telegram Log Group: {e}")
            return False
