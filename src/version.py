"""
Central Application Release Version & Build Traceability Module (AB-V14-P0-RECONCILE-VERSION-CONTROL-01).

Maintains strict separation across three distinct version namespaces:
1. Application Release Version (`APP_RELEASE_VERSION`, e.g., `14.0.1-rc.1`):
   Single machine-readable source of truth in root `VERSION` file (SemVer).
   Changes ONLY on code-changing PRs; never incremented when operator edits WebUI `.env` parameters.
2. Runtime Configuration Fingerprint (`v-<8-char-sha256>`):
   Computed dynamically by `src/engine/config_manager.py` from `.env` trading parameters.
3. Parser Rule Version (`PARSER_RULE_VERSION`, e.g., `BRAIN-v3.2`):
   Identifies the signal parser rule/validation schema generation.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(APP_ROOT, "VERSION")
CHANGELOG_FILE = os.path.join(APP_ROOT, "CHANGELOG.md")
RELEASE_MANIFEST_FILE = os.path.join(APP_ROOT, "RELEASE_MANIFEST.json")

DEFAULT_APP_VERSION = "14.0.1-rc.1"
HISTORICAL_V14_BASELINE_VERSION = "14.0.0"
PARSER_RULE_VERSION = "BRAIN-v3.2"

SEMVER_PATTERN = re.compile(
    r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+(?P<build>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


def parse_semver(version_str: str) -> Tuple[int, int, int, Tuple[Any, ...]]:
    """
    Parses a SemVer string into a comparable tuple `(major, minor, patch, prerelease_key)`.
    Per SemVer 2.0.0, a normal version (`14.0.1`) has higher precedence than a pre-release (`14.0.1-rc.1`),
    and both have higher precedence than `14.0.0`.
    """
    cleaned = str(version_str or "").strip()
    m = SEMVER_PATTERN.match(cleaned)
    if not m:
        raise ValueError(f"Invalid SemVer string: {version_str!r}")
    major = int(m.group("major"))
    minor = int(m.group("minor"))
    patch = int(m.group("patch"))
    pre = m.group("prerelease")
    if not pre:
        pre_key: Tuple[Any, ...] = (1,)
    else:
        parts: List[Any] = []
        for token in pre.split("."):
            if token.isdigit():
                parts.append((0, int(token)))
            else:
                parts.append((1, token))
        pre_key = (0, tuple(parts))
    return (major, minor, patch, pre_key)


def is_valid_semver(version_str: str) -> bool:
    try:
        parse_semver(version_str)
        return True
    except ValueError:
        return False


def compare_semver(ver_a: str, ver_b: str) -> int:
    """Returns -1 if ver_a < ver_b, 0 if equal, +1 if ver_a > ver_b."""
    ka = parse_semver(ver_a)
    kb = parse_semver(ver_b)
    if ka < kb:
        return -1
    if ka > kb:
        return 1
    return 0


def get_app_version(version_path: Optional[str] = None) -> str:
    """Reads the authoritative application release SemVer from root `VERSION`."""
    path = version_path or VERSION_FILE
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                val = f.read().strip()
            if val and is_valid_semver(val):
                return val
        except Exception:
            pass
    return DEFAULT_APP_VERSION


def get_git_commit_sha(short: bool = False, repo_root: Optional[str] = None) -> str:
    """
    Resolves the built/deployed Git commit dynamically from environment or `git rev-parse HEAD`.
    Never embeds a self-referential commit hash inside a tracked manifest.
    """
    env_sha = os.environ.get("AUDITBOT_BUILD_COMMIT", "").strip() or os.environ.get("GITHUB_SHA", "").strip()
    if env_sha:
        return env_sha[:8] if short else env_sha
    cwd = repo_root or APP_ROOT
    try:
        cmd = ["git", "-C", cwd, "rev-parse", "--short=8", "HEAD"] if short else ["git", "-C", cwd, "rev-parse", "HEAD"]
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True, timeout=2.0).strip()
        if out:
            return out
    except Exception:
        pass
    return "unknown"


def get_release_manifest(manifest_path: Optional[str] = None) -> Dict[str, Any]:
    path = manifest_path or RELEASE_MANIFEST_FILE
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}


def get_release_summary(config_fingerprint: Optional[str] = None) -> Dict[str, Any]:
    """Returns structured release & namespace metadata for CLI, WebUI, and startup diagnostics."""
    manifest = get_release_manifest()
    ver = get_app_version()
    return {
        "app_release_version": ver,
        "previous_version": manifest.get("previous_version", HISTORICAL_V14_BASELINE_VERSION),
        "target_stable_version": manifest.get("target_stable_version", "14.0.1"),
        "patch_identifier": manifest.get("patch_identifier", "AB-V14-P0-RECONCILE-VERSION-CONTROL-01"),
        "parent_main_baseline_sha": manifest.get("parent_main_baseline_sha", "29c6515b8ce448ff54bf1f3024f2e7c9027c8896"),
        "git_commit": get_git_commit_sha(short=False),
        "git_commit_short": get_git_commit_sha(short=True),
        "parser_rule_version": PARSER_RULE_VERSION,
        "config_fingerprint": config_fingerprint or "v-UNKNOWN",
        "verification_status": manifest.get("verification_status", "RC_VERIFIED_AWAITING_OWNER_REVIEW"),
    }


def _run_git_checked(root: str, args: List[str]) -> Tuple[bool, str]:
    try:
        res = subprocess.run(
            ["git", "-C", root] + args,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
        if res.returncode != 0:
            err_msg = (res.stderr or res.stdout or f"git {' '.join(args)} exited with {res.returncode}").strip()
            return False, err_msg
        return True, res.stdout.strip()
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def verify_release_version_governance(
    repo_root: Optional[str] = None,
    base_ref: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """
    P1-03: Fail-closed release version governance verifier for CI and release gating:
    1. Root `VERSION` exists and contains valid SemVer.
    2. `RELEASE_MANIFEST.json` exists, matches `VERSION`, has `version > previous_version`,
       records `parent_main_baseline_sha` and `patch_identifier`, and all `changed_paths` exist.
    3. `CHANGELOG.md` exists and contains a dedicated entry `## [<version>]` for the current `VERSION`.
    4. When `base_ref` is supplied (`base_ref is not None`), strictly requires:
       - A valid Git working tree (`git rev-parse --is-inside-work-tree`)
       - Resolvable `HEAD^{commit}` and `<base_ref>^{commit}`
       - Successful `git merge-base <base_commit> <head_commit>`
       - Successful `git diff --name-only <merge_base>..HEAD`
       - Proper SemVer increment (`VERSION > base VERSION`, or `VERSION > 14.0.0` on verified initial VERSION adoption)
         and updated `VERSION`, `CHANGELOG.md`, and `RELEASE_MANIFEST.json` whenever code files change.
       Never catches and ignores Git errors on an explicit `base_ref`.
    """
    root = repo_root or APP_ROOT
    errors: List[str] = []

    v_file = os.path.join(root, "VERSION")
    c_file = os.path.join(root, "CHANGELOG.md")
    m_file = os.path.join(root, "RELEASE_MANIFEST.json")

    if not os.path.exists(v_file):
        errors.append("Missing root VERSION file")
        return False, errors

    try:
        with open(v_file, "r", encoding="utf-8") as f:
            ver_str = f.read().strip()
    except Exception as exc:
        errors.append(f"Cannot read VERSION file: {exc}")
        return False, errors

    if not is_valid_semver(ver_str):
        errors.append(f"Root VERSION contains invalid SemVer: {ver_str!r}")
        return False, errors

    manifest: Dict[str, Any] = {}
    if not os.path.exists(m_file):
        errors.append("Missing root RELEASE_MANIFEST.json")
    else:
        try:
            with open(m_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                errors.append("RELEASE_MANIFEST.json root must be a JSON object")
            else:
                manifest = loaded
                m_ver = str(manifest.get("version") or "").strip()
                prev_ver = str(manifest.get("previous_version") or "").strip()
                if m_ver != ver_str:
                    errors.append(f"RELEASE_MANIFEST.json version ({m_ver!r}) != VERSION ({ver_str!r})")
                if not is_valid_semver(prev_ver):
                    errors.append(f"RELEASE_MANIFEST.json previous_version ({prev_ver!r}) is not valid SemVer")
                elif compare_semver(ver_str, prev_ver) <= 0:
                    errors.append(f"Current version ({ver_str}) must be strictly greater than previous_version ({prev_ver})")
                if not str(manifest.get("parent_main_baseline_sha") or "").strip():
                    errors.append("RELEASE_MANIFEST.json missing parent_main_baseline_sha")
                if not str(manifest.get("patch_identifier") or "").strip():
                    errors.append("RELEASE_MANIFEST.json missing patch_identifier")
                changed_paths = manifest.get("changed_paths")
                if not isinstance(changed_paths, list) or not changed_paths:
                    errors.append("RELEASE_MANIFEST.json changed_paths must be a non-empty list")
                else:
                    for cp in changed_paths:
                        if not os.path.exists(os.path.join(root, str(cp))):
                            errors.append(f"RELEASE_MANIFEST.json changed_path does not exist: {cp}")
        except Exception as e:
            errors.append(f"Failed to parse RELEASE_MANIFEST.json: {e}")

    if not os.path.exists(c_file):
        errors.append("Missing root CHANGELOG.md")
    else:
        try:
            with open(c_file, "r", encoding="utf-8") as f:
                clog = f.read()
            if f"## [{ver_str}]" not in clog:
                errors.append(f"CHANGELOG.md missing section header '## [{ver_str}]'")
        except Exception as exc:
            errors.append(f"Failed to read CHANGELOG.md: {exc}")

    if base_ref is not None:
        clean_base_ref = str(base_ref).strip()
        if not clean_base_ref:
            errors.append("Explicit --base-ref cannot be empty")
            return False, errors

        ok_wt, wt_out = _run_git_checked(root, ["rev-parse", "--is-inside-work-tree"])
        if not ok_wt or wt_out.lower() != "true":
            errors.append(f"Directory '{root}' is not a valid Git working tree: {wt_out}")
            return False, errors

        ok_head, head_sha = _run_git_checked(root, ["rev-parse", "--verify", "HEAD^{commit}"])
        if not ok_head or not head_sha:
            errors.append(f"Cannot resolve HEAD commit in '{root}': {head_sha}")
            return False, errors

        ok_base, base_sha = _run_git_checked(root, ["rev-parse", "--verify", f"{clean_base_ref}^{{commit}}"])
        if not ok_base or not base_sha:
            errors.append(f"Invalid or unresolvable Git base reference '{clean_base_ref}': {base_sha}")
            return False, errors

        ok_mb, merge_base_sha = _run_git_checked(root, ["merge-base", base_sha, head_sha])
        if not ok_mb or not merge_base_sha:
            errors.append(
                f"Cannot compute git merge-base between '{clean_base_ref}' ({base_sha[:8]}) and HEAD ({head_sha[:8]}): {merge_base_sha}"
            )
            return False, errors

        ok_diff, diff_out = _run_git_checked(root, ["diff", "--name-only", f"{merge_base_sha}..{head_sha}"])
        if not ok_diff:
            errors.append(f"git diff failed between '{clean_base_ref}' ({merge_base_sha[:8]}) and HEAD ({head_sha[:8]}): {diff_out}")
            return False, errors

        diff_files = [line.strip() for line in diff_out.splitlines() if line.strip()]

        # Resolve VERSION at base commit (or verified initial VERSION adoption from 14.0.0 baseline)
        ok_base_has_ver, _ = _run_git_checked(root, ["cat-file", "-e", f"{base_sha}:VERSION"])
        if ok_base_has_ver:
            ok_show, base_ver_raw = _run_git_checked(root, ["show", f"{base_sha}:VERSION"])
            if not ok_show:
                errors.append(f"Failed to read VERSION from base commit {base_sha[:8]}: {base_ver_raw}")
                return False, errors
            base_ver = base_ver_raw.strip()
            if not is_valid_semver(base_ver):
                errors.append(f"Base commit {base_sha[:8]} contains invalid SemVer in VERSION: {base_ver!r}")
                return False, errors
        else:
            # Valid existing base commit lacks VERSION: permitted only for documented initial migration from 14.0.0
            base_ver = HISTORICAL_V14_BASELINE_VERSION
            if str(manifest.get("previous_version") or "").strip() != HISTORICAL_V14_BASELINE_VERSION:
                errors.append(
                    f"Base commit {base_sha[:8]} has no VERSION file, and RELEASE_MANIFEST.json previous_version "
                    f"({manifest.get('previous_version')!r}) does not match historical baseline {HISTORICAL_V14_BASELINE_VERSION!r}"
                )

        code_changed = any(
            df.startswith(("src/", "deploy/", "tests/", ".github/"))
            or df in ("bot", "run.py", "requirements.txt", "auditbot_dhan_readonly_order_probe.py")
            for df in diff_files
        )
        if code_changed:
            if "VERSION" not in diff_files:
                errors.append(f"Code files changed relative to '{clean_base_ref}', but VERSION was not updated in diff")
            if "CHANGELOG.md" not in diff_files:
                errors.append(f"Code files changed relative to '{clean_base_ref}', but CHANGELOG.md was not updated in diff")
            if "RELEASE_MANIFEST.json" not in diff_files:
                errors.append(f"Code files changed relative to '{clean_base_ref}', but RELEASE_MANIFEST.json was not updated in diff")
            if compare_semver(ver_str, base_ver) <= 0:
                errors.append(
                    f"New VERSION ({ver_str}) must be strictly greater than base '{clean_base_ref}' version ({base_ver})"
                )

    return len(errors) == 0, errors


APP_RELEASE_VERSION = get_app_version()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="AuditBot Release Version & Governance Verifier")
    parser.add_argument("--verify", action="store_true", help="Verify VERSION, CHANGELOG.md, and RELEASE_MANIFEST.json")
    parser.add_argument("--base-ref", default=None, help="Optional Git base ref for PR version-bump verification")
    parser.add_argument("--json", action="store_true", help="Print full release summary as JSON")
    args = parser.parse_args(argv)

    if args.verify:
        ok, errs = verify_release_version_governance(base_ref=args.base_ref)
        if not ok:
            for err in errs:
                print(f"[VERSION-CHECK FAILED] {err}", file=sys.stderr)
            return 1
        summary = get_release_summary()
        print(
            f"[VERSION-CHECK OK] version={summary['app_release_version']} "
            f"previous={summary['previous_version']} patch={summary['patch_identifier']} "
            f"commit={summary['git_commit_short']}"
        )
        return 0

    if args.json:
        print(json.dumps(get_release_summary(), indent=2))
        return 0

    print(get_app_version())
    return 0


if __name__ == "__main__":
    sys.exit(main())
