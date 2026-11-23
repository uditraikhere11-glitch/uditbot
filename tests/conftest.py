"""Load the same explicit isolation perimeter before pytest collects test modules."""
import os

os.environ.setdefault("AUDITBOT_TEST_ISOLATION", "1")
os.environ.setdefault("UDITBOT_OFFLINE_TESTS", "1")

# Importing the test package installs the existing Linux seccomp offline guard.
import tests  # noqa: F401
