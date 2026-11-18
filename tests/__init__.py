import os
import sys

if sys.platform == "linux" and (os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("UDITBOT_OFFLINE_TESTS") == "1"):
    from tests.offline_network import block_network

    block_network()
