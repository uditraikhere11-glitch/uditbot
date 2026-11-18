#!/usr/bin/env python3
"""Generate exact full source replacements for two audited entrypoints.

Required input: GitHub main files at 11d2c4c, identified by Git blob SHA.
Writes to a separate output tree; NEVER edits the input repository. Rejects
source drift, missing anchors, or accidental duplicate injection.
"""
from __future__ import annotations
import argparse
import hashlib
from pathlib import Path

GIT_BASE = '11d2c4c3c48264a03e498761b1e5158c8eb6706a'
BLOBS = {
    'src/engine/webui_update.py': '7009ebf3e9e880f034434e7fe6f6c95f6d1b12ad',
    'src/control_center_backend.py': '75b120046ac00ab91bb6aa94873d992f3b661a50',
}

WEBUI_OLD = '''    env_path = os.path.abspath(env_path)
    if os.environ.get("UDITBOT_TRACKER_V2_ENABLE") == "1":
        from src.engine.tracker_v2_integration import submit_v2_webui_patch
'''
WEBUI_NEW = '''    env_path = os.path.abspath(env_path)
    # PR-V2-ACTIVATE: a mixed legacy WebUI / V2 trading worker must never
    # save configuration through the wrong coordinator or bypass its fence.
    from src.engine.tracker_v2_dual_service import (
        assert_consistent_save_route, DualServiceMismatch,
    )
    try:
        assert_consistent_save_route(
            tracker.data_dir,
            web_v2=os.environ.get("UDITBOT_TRACKER_V2_ENABLE") == "1",
        )
    except DualServiceMismatch as exc:
        raise ConfigurationConflict("V2 routing safety gate: " + str(exc)) from None
    if os.environ.get("UDITBOT_TRACKER_V2_ENABLE") == "1":
        from src.engine.tracker_v2_integration import submit_v2_webui_patch
'''

HEALTH_OLD = '''    verified = status.get("runtime_verified") is True
    check("Active mode/version", "PASS" if verified else "UNKNOWN", "Verified" if verified else "Running process version not verified")
    stop_file = os.path.join(data_dir, "emergency_stop.json")
'''
HEALTH_NEW = '''    verified = status.get("runtime_verified") is True
    check("Active mode/version", "PASS" if verified else "UNKNOWN", "Verified" if verified else "Running process version not verified")
    # Positive service/process verification alone cannot override a saved
    # owner STOP. This diagnostic is read-only and never edits control files.
    from src.engine.tracker_v2_health import owner_intent_health
    intent = owner_intent_health(str(state), status.get("owner_intent"))
    check("Owner service intent", intent.status, intent.message)
    stop_file = os.path.join(data_dir, "emergency_stop.json")
'''

REPL = {
    'src/engine/webui_update.py': (WEBUI_OLD, WEBUI_NEW),
    'src/control_center_backend.py': (HEALTH_OLD, HEALTH_NEW),
}


def git_blob_hash(raw: bytes) -> str:
    return hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()


def replace_exact(path: str, raw: bytes, *, check_blob: bool = True) -> bytes:
    if check_blob and git_blob_hash(raw) != BLOBS[path]:
        raise ValueError(f'Baseline Git blob mismatch for {path}. Do not force/apply.')
    old, new = REPL[path]
    text = raw.decode('utf-8')
    if text.count(old) != 1:
        raise ValueError(f'Expected integration anchor does not match uniquely: {path}')
    return text.replace(old, new, 1).encode('utf-8')


def generate(repo: Path, destination: Path) -> None:
    for name in BLOBS:
        source = repo / name
        replacement = replace_exact(name, source.read_bytes())
        output = destination / name
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            raise FileExistsError(f'Refusing overwrite: {output}')
        output.write_bytes(replacement)
        print('FULL_REPLACEMENT:', name, 'sha256=' + hashlib.sha256(replacement).hexdigest())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('repo', type=Path, help='exact GitHub 11d2c4c source checkout')
    parser.add_argument('destination', type=Path, help='separate target folder')
    args = parser.parse_args()
    generate(args.repo.resolve(), args.destination.resolve())
