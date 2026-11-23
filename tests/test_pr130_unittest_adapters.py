"""Run all PR130 function-style adversarial probes under the repository's unittest CI.

Existing CI runs unittest discovery rather than pytest discovery. This adapter
ensures every new positive/negative control actually executes in release CI.
"""
import unittest
from tests import (
    test_owner_service_admission_pr130,
    test_pr130_owner_cancel,
    test_pr130_service_closed_lot,
)


class PR130ReleaseAcceptance(unittest.TestCase):
    """Generated one independent test case for each explicit scenario."""


def _add_module(mod, prefix):
    for key in sorted(vars(mod)):
        if not key.startswith("test_"):
            continue
        function = getattr(mod, key)
        if not callable(function):
            continue

        def _run(self, func=function):
            func()

        _run.__name__ = "test_" + prefix + "_" + key[5:]
        setattr(PR130ReleaseAcceptance, _run.__name__, _run)


_add_module(test_owner_service_admission_pr130, "admission")
_add_module(test_pr130_owner_cancel, "cancellation")
_add_module(test_pr130_service_closed_lot, "closed_lot")
