"""Explicit stopped-service adapter for synthetic offline WebUI tests."""
import os
from functools import wraps
from unittest.mock import patch

def stopped_service_test(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        if os.environ.get("AUDITBOT_TEST_ISOLATION") != "1":
            raise RuntimeError("Stopped fixture requires offline test isolation")

        from src.engine import simple_save_apply as direct
        import src.control_center_backend as control

        original = direct.apply_settings

        def stopped_apply(*a, **kw):
            kw = dict(kw)
            kw["state_reader"] = lambda: "inactive"
            kw["restart"] = lambda: None
            return original(*a, **kw)

        with (
            patch.object(direct, "apply_settings", side_effect=stopped_apply),
            patch.object(control, "apply_settings", side_effect=stopped_apply),
        ):
            return fn(self, *args, **kwargs)

    return wrapped
