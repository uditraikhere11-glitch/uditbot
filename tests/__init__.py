import os
import sys

# Explicit opt-in for unit-test entry points (not triggered by importing unittest
# or pytest in the production application). CI also sets this in its environment.
os.environ.setdefault("AUDITBOT_TEST_ISOLATION", "1")
os.environ.setdefault("UDITBOT_OFFLINE_TESTS", "1")

if sys.platform == "linux" and (os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("UDITBOT_OFFLINE_TESTS") == "1"):
    from tests.offline_network import block_network

    block_network()
