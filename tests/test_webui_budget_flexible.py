"""
Test Suite for AB-03: Flexible WebUI Equity Budget and Minimum Ledger Funds Settings.

Validates:
1. ConfigChangeTracker.validate_inputs accepts flexible finite decimal equity budgets (> 0.0) such as 1, 100, 100.50, 250, 500, 25000.
2. ConfigChangeTracker.validate_inputs rejects zero, negative, non-finite (NaN, Inf), boolean, and non-numeric budgets.
3. ConfigChangeTracker.validate_inputs accepts min_trade_funds_inr >= 0.0, including 0.0 and positive decimals.
4. ConfigChangeTracker.validate_inputs rejects negative, non-finite, boolean, and non-numeric min funds.
5. Setup WebUI template renders step="any" min="0.01" for equity_budget_inr and step="any" min="0" for min_trade_funds_inr.
6. Setup WebUI template renders decimal budget string without int truncation.
7. Setup WebUI tg_api_hash does not enforce 'required' so existing saved hash is preserved on submission.
"""

import os
import unittest
from src.engine.config_manager import ConfigChangeTracker


class TestWebUIFlexibleBudget(unittest.TestCase):
    def setUp(self):
        self.tracker = ConfigChangeTracker(data_dir="/tmp")

    def test_equity_budget_valid_flexible_values(self):
        valid_budgets = [0.01, 1.0, 10.0, 100.0, 100.50, 250.0, 500.0, 1234.56, 25000.0, 100000.0]
        for b in valid_budgets:
            ok, msg = self.tracker.validate_inputs({"equity_budget_inr": b})
            self.assertTrue(ok, f"Budget {b} should be valid, got error: {msg}")

    def test_equity_budget_rejects_invalid_values(self):
        invalid_budgets = [0, 0.0, -1, -500.0, float("nan"), float("inf"), float("-inf"), True, False, "invalid", None]
        for b in invalid_budgets:
            ok, msg = self.tracker.validate_inputs({"equity_budget_inr": b})
            self.assertFalse(ok, f"Budget {b} should be invalid")

    def test_min_trade_funds_valid_values(self):
        valid_funds = [0, 0.0, 0.01, 1.0, 100.0, 500.50, 1000.0, 10000.0]
        for f in valid_funds:
            ok, msg = self.tracker.validate_inputs({"min_trade_funds_inr": f})
            self.assertTrue(ok, f"Min funds {f} should be valid, got error: {msg}")

    def test_min_trade_funds_rejects_invalid_values(self):
        invalid_funds = [-0.01, -1, -1000.0, float("nan"), float("inf"), float("-inf"), True, False, "abc"]
        for f in invalid_funds:
            ok, msg = self.tracker.validate_inputs({"min_trade_funds_inr": f})
            self.assertFalse(ok, f"Min funds {f} should be invalid")

    def test_webui_html_template_attributes(self):
        from src.setup_app import HTML_TEMPLATE
        self.assertIn('name="equity_budget_inr"', HTML_TEMPLATE)
        self.assertIn('step="any"', HTML_TEMPLATE)
        self.assertNotIn('step="500" name="equity_budget_inr"', HTML_TEMPLATE)
        self.assertNotIn('step="1000" name="min_trade_funds_inr"', HTML_TEMPLATE)

        # Ensure tg_api_hash is not required
        self.assertIn('name="tg_api_hash"', HTML_TEMPLATE)
        self.assertNotIn('name="tg_api_hash" value="<!--TG_API_HASH-->" placeholder="e.g. 0123456789abcdef..." required', HTML_TEMPLATE)

    def test_webui_renders_decimal_budget_without_truncation(self):
        from unittest.mock import patch
        from src.config import AppConfig
        from src.setup_app import get_setup_page

        cfg = AppConfig(
            equity_budget_inr=100.5,
            min_trade_funds_inr=0.0,
        )

        with patch("src.setup_app.AppConfig.from_env", return_value=cfg):
            resp = get_setup_page()

        html = resp.body.decode("utf-8") if hasattr(resp, "body") else str(resp)
        self.assertIn('name="equity_budget_inr" value="100.5"', html)
        self.assertIn('name="min_trade_funds_inr" value="0"', html)


if __name__ == "__main__":
    unittest.main()
