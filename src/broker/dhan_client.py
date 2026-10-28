"""
DhanHQ API v2 Client Interface.
Handles authentication, token validity checks, market quotes (LTP),
order submission (strictly disabled in TEST mode), and Forever OCO protection legs.
"""

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple
import requests
from src.config import AppConfig
from src.models import OrderRequest, ProtectionLevels, TradingMode

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dhan.co/v2"


class DhanClient:
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.session = requests.Session()
        self.token = self._load_token()

    def _load_token(self) -> str:
        """Loads access token from disk or config."""
        if self.cfg.dhan_access_token:
            return self.cfg.dhan_access_token
        if os.path.exists(self.cfg.dhan_token_file):
            try:
                with open(self.cfg.dhan_token_file, "r") as f:
                    return f.read().strip()
            except Exception as e:
                logger.error(f"Error reading Dhan token file: {e}")
        return ""

    def _headers(self) -> Dict[str, str]:
        return {
            "access-token": self.token,
            "client-id": self.cfg.dhan_client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def is_token_error(self, status_code: int, response_text: str) -> bool:
        """Detects Dhan expired or invalid token errors."""
        text = response_text.lower()
        return status_code in (401, 403) or "dh-901" in text or "dh-906" in text or "invalid token" in text

    def get_fund_limits(self) -> Tuple[bool, float, str]:
        """
        Retrieves available ledger balance from Dhan.
        Returns: (success, available_balance, message)
        """
        if self.cfg.trading_mode == TradingMode.TEST and not self.token:
            # Standalone test default when credentials are not configured on test machine
            return True, 100000.0, "Test mode default balance"

        url = f"{BASE_URL}/funds"
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                avail = float(data.get("availabelBalance", 0.0))
                return True, avail, "Funds retrieved successfully"
            elif self.is_token_error(resp.status_code, resp.text):
                return False, 0.0, f"Dhan auth error ({resp.status_code}): {resp.text}"
            else:
                return False, 0.0, f"Dhan funds API error ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, 0.0, f"HTTP exception fetching funds: {e}"

    def get_ltp(self, exchange_segment: str, security_id: str) -> Tuple[bool, float, str]:
        """
        Fetches real-time Last Traded Price (LTP) from Dhan.
        """
        url = f"{BASE_URL}/marketfeed/ltp"
        payload = {
            "instruments": [
                {
                    "exchangeSegment": exchange_segment,
                    "securityId": str(security_id)
                }
            ]
        }
        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                # Parse structure: data[exchangeSegment][securityId]["last_price"]
                seg_data = data.get("data", {}).get(exchange_segment, {})
                sec_data = seg_data.get(str(security_id), {})
                ltp = float(sec_data.get("last_price", 0.0))
                if ltp > 0:
                    return True, ltp, "LTP retrieved successfully"
                return False, 0.0, f"LTP is 0 or missing in response: {resp.text}"
            elif self.is_token_error(resp.status_code, resp.text):
                return False, 0.0, f"Dhan auth error ({resp.status_code}): {resp.text}"
            else:
                return False, 0.0, f"Dhan LTP API error ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, 0.0, f"HTTP exception fetching LTP: {e}"

    def place_order(self, order_req: OrderRequest) -> Tuple[bool, Optional[str], str]:
        """
        Places a BUY LIMIT order on Dhan.
        STRICT SAFETY RULE: In TEST mode, live orders are strictly blocked.
        """
        if self.cfg.trading_mode == TradingMode.TEST:
            return False, None, "ORDER BLOCKED: System is in TEST mode (Zero live orders allowed)"

        url = f"{BASE_URL}/orders"
        payload = {
            "dhanClientId": self.cfg.dhan_client_id,
            "correlationId": order_req.correlation_id,
            "transactionType": "BUY",
            "exchangeSegment": order_req.exchange_segment,
            "productType": order_req.product_type.value,
            "orderType": "LIMIT",
            "validity": "DAY",
            "securityId": str(order_req.security_id),
            "quantity": int(order_req.quantity),
            "price": float(order_req.price),
            "triggerPrice": 0.0,
            "disclosedQuantity": 0,
            "afterMarketOrder": False,
        }

        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            if resp.status_code in (200, 201):
                data = resp.json()
                order_id = str(data.get("orderId", ""))
                return True, order_id, "Order placed successfully on Dhan"
            elif self.is_token_error(resp.status_code, resp.text):
                return False, None, f"Dhan auth error ({resp.status_code}): {resp.text}"
            else:
                return False, None, f"Dhan order placement rejected ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, None, f"HTTP exception placing order: {e}"

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        """Cancels resting order on Dhan."""
        if self.cfg.trading_mode == TradingMode.TEST:
            return True, "TEST mode: simulated cancel acknowledgment"

        url = f"{BASE_URL}/orders/{order_id}"
        try:
            resp = self.session.delete(url, headers=self._headers(), timeout=5)
            if resp.status_code == 200:
                return True, "Order cancelled successfully"
            return False, f"Failed to cancel order ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, f"HTTP exception cancelling order: {e}"

    def place_forever_oco(
        self,
        correlation_id: str,
        security_id: str,
        exchange_segment: str,
        product_type: str,
        levels: ProtectionLevels,
    ) -> Tuple[bool, Optional[str], str]:
        """
        Places a Dhan Forever OCO protection order (Target + SL) based on executed fill price.
        """
        if self.cfg.trading_mode == TradingMode.TEST:
            return False, None, "FOREVER OCO BLOCKED: System is in TEST mode"

        url = f"{BASE_URL}/forever/orders"
        payload = {
            "dhanClientId": self.cfg.dhan_client_id,
            "correlationId": correlation_id,
            "orderFlag": "OCO",
            "transactionType": "SELL",
            "exchangeSegment": exchange_segment,
            "productType": product_type,
            "securityId": str(security_id),
            "quantity": int(levels.quantity),
            "price": float(levels.target_price),
            "triggerPrice": float(levels.sl_trigger),
            "targetPrice": float(levels.target_price),
            "stopLossPrice": float(levels.sl_trigger),
        }

        try:
            resp = self.session.post(url, headers=self._headers(), json=payload, timeout=5)
            if resp.status_code in (200, 201):
                data = resp.json()
                forever_id = str(data.get("orderId", ""))
                return True, forever_id, "Forever OCO placed successfully"
            return False, None, f"Forever OCO rejected ({resp.status_code}): {resp.text}"
        except Exception as e:
            return False, None, f"HTTP exception placing Forever OCO: {e}"
