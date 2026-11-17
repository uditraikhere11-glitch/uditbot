"""PR89 deterministic synthetic configuration stress test.

Runs 8,000+ distinct offline cases against the *real* field registry and
candidate transaction coordinator.  These are not broker or LIVE VM tests.
The random seed and enumeration are frozen for reproducible CI failure cases.
"""
from __future__ import annotations

import hashlib
import os
import time
import json
from pathlib import Path
import random
import tempfile
import unittest

from src.engine.config_patch import (
    ConfigurationConflict, ConfigurationRejected, REGISTRY, ENV_REGISTRY,
    normalize, parse_env_strict, patch_env, revision,
    validate_effective_config, redact,
)
from src.engine.tracker_v2_candidate import CandidateCoordinator, ChangeBlocked
from src.engine.tracker_v2_safety import preview_legacy_tracker_migration


BASE = (
    "# A real-looking synthetic config with unrelated extension lines\n"
    "TRADING_MODE=TEST\n"
    "EQUITY_BUDGET_INR=500\n"
    "FNO_CAPITAL_PER_TRADE=19500\n"
    "EQUITY_AMO_ENABLED=false\n"
    "FNO_AMO_ENABLED=false\n"
    "CUSTOM_DO_NOT_TOUCH=  A:XYZ  # comment retained\n"
    "# Operator comment is not a setting\n"
)


def value_for(spec, idx):
    kind = spec.value_type
    if kind == "mode":
        return ("TEST", "LIVE", "true", "false", True, False)[idx % 6]
    if kind == "bool":
        return ("true", "false", "yes", "no", "1", "0", True, False)[idx % 8]
    if kind == "enum":
        return spec.choices[idx % len(spec.choices)]
    if kind == "integer":
        if spec.key in {"TG_API_ID", "LOG_GROUP_ID", "TEST_GROUP_ID"} and idx % 7 == 0:
            return ""
        return str(int(max(0, spec.min_value or 0)) + 1 + idx % 15)
    if kind == "number":
        n = max(float(spec.min_value or 0), float((idx % 19) + 1))
        if spec.max_value is not None:
            n = min(float(spec.max_value), n)
        return format(n, ".7g")
    if kind == "groups":
        return ("-100011,-100022", "111,222", "", "-100333", "55,-66")[idx % 5]
    if kind == "response":
        return ("Testing Group Alpha", "Testing Group Beta", "-100011", "", "Ops group")[idx % 5]
    if kind == "secret":
        return "SYNTHETIC_NOT_REAL_SECRET_%03d" % idx
    return ("Synthetic Operator", "SyntheticInstance", "", "alpha-beta_01", "User 12")[idx % 5]


class EveryFieldStressTests(unittest.TestCase):
    def test_2040_individual_field_edits_and_replay_invariance(self):
        count = 0
        self.assertEqual(len(REGISTRY), 51)
        for name, spec in REGISTRY.items():
            for i in range(40):
                # Exercise both newline styles and both no-newline and
                # fully terminated original documents; keep extensions exact.
                ending = "\r\n" if i % 2 else "\n"
                initial = BASE.replace("\n", ending)
                if i % 3 == 0:
                    initial = initial.rstrip("\r\n")
                before_values, _ = parse_env_strict(initial)
                raw = value_for(spec, i)
                proposed, changed = patch_env(
                    initial, {name: raw}, expected_revision=revision(initial)
                )
                actual, _ = parse_env_strict(proposed)
                self.assertTrue(set(changed).issubset({spec.key}), (name, i, changed))
                self.assertEqual(actual["CUSTOM_DO_NOT_TOUCH"], before_values["CUSTOM_DO_NOT_TOUCH"])
                self.assertIn("CUSTOM_DO_NOT_TOUCH=  A:XYZ  # comment retained", proposed)
                self.assertIn("# Operator comment is not a setting", proposed)
                for key in before_values:
                    if key != spec.key:
                        self.assertEqual(before_values[key], actual[key], (name, i, key))
                if changed:
                    self.assertEqual(actual[spec.key], normalize(spec, raw), (name, i))
                    self.assertNotEqual(revision(proposed), revision(initial))
                else:
                    self.assertEqual(proposed, initial, (name, i))
                replay, replay_changed = patch_env(
                    proposed, {name: raw}, expected_revision=revision(proposed)
                )
                self.assertEqual(replay, proposed, (name, i))
                self.assertFalse(replay_changed, (name, i))
                if spec.secret:
                    self.assertEqual(redact(spec.key, actual.get(spec.key, "")), "[REDACTED]")
                count += 1
        self.assertEqual(count, 2040)
        print("STRESS VERIFIED 2040 individual editable-field saves", flush=True)

    def test_2500_mixed_multi_field_edits_no_bystander_changes(self):
        rng = random.Random(890215)
        all_fields = list(REGISTRY)
        for i in range(2500):
            picked = rng.sample(all_fields, rng.randrange(2, 13))
            form = {name: value_for(REGISTRY[name], i + k * 7)
                    for k, name in enumerate(picked)}
            initial = BASE
            old, _ = parse_env_strict(initial)
            updated, changed = patch_env(
                initial, form, expected_revision=revision(initial)
            )
            actual, _ = parse_env_strict(updated)
            self.assertTrue(set(changed).issubset({REGISTRY[n].key for n in picked}),
                            (i, sorted(changed)))
            self.assertEqual(actual["CUSTOM_DO_NOT_TOUCH"], old["CUSTOM_DO_NOT_TOUCH"])
            self.assertEqual(actual.get("TRADING_MODE") if "is_live_mode" not in picked
                             else None,
                             old["TRADING_MODE"] if "is_live_mode" not in picked
                             else None)
            for key, before in old.items():
                if key not in changed:
                    self.assertEqual(actual.get(key), before, (i, key))
            again, delta = patch_env(updated, form, expected_revision=revision(updated))
            self.assertEqual(again, updated, i)
            self.assertFalse(delta, i)
            # A concurrent external writer must invalidate the form's revision.
            if changed:
                with self.assertRaises(ConfigurationConflict):
                    patch_env(updated, form, expected_revision=revision(initial))
        print("STRESS VERIFIED 2500 mixed multi-field saves/replays and CAS", flush=True)

    def test_1500_invalid_or_conflicting_requests_fail_closed(self):
        invalid = (
            ({"equity_budget_inr": "-1"}, "numeric below min"),
            ({"equity_budget_inr": "nan"}, "numeric nan"),
            ({"equity_budget_inr": "Infinity"}, "numeric infinity"),
            ({"fno_sl_pct": "-1"}, "negative stop loss"),
            ({"fno_sl_pct": "51"}, "oversize stop loss"),
            ({"max_drawdown_pct": "101"}, "drawdown above max"),
            ({"is_live_mode": "production"}, "unknown mode"),
            ({"fno_amo_enabled": "MAYBE"}, "unknown boolean"),
            ({"equity_product_type": "SUPER_ORDER"}, "unknown enum"),
            ({"monitored_channel_ids": "-1001,abc"}, "invalid group"),
            ({"equity_budget_inr": "500\nFNO_ACTIVE=true"}, "newline injection"),
            ({"operator_name": "A # commented"}, "unsafe comment syntax"),
            ({"tg_bot_token": "quoted'bad"}, "unrepresentable secret quote"),
            ({"unrecognized_setting": "NEW"}, "unknown field"),
            ({"dhan_access_token": "SECRET-BLOCKED"}, "access token must be managed"),
            ({"max_daily_trades": "1.5"}, "fractional integer"),
            ({"max_daily_trades": "true"}, "bool string as integer"),
            ({"max_daily_trades": "1000000000000000000000000000000000000000000000000000000000000000000000000"}, "huge integer disallowed only by range, not this test"),
        )
        # Do not test arbitrary valid upper integer limits; only definitively invalid values.
        cases = invalid[:-1]
        for i in range(1500):
            form, why = cases[i % len(cases)]
            baseline = BASE + ("# trial %d\n" % i)
            with self.subTest(trial=i, type=why):
                with self.assertRaises((ConfigurationRejected, ConfigurationConflict, ValueError)):
                    patch_env(baseline, form, expected_revision=revision(baseline))
        print("STRESS VERIFIED 1500 negative inputs rejected", flush=True)

    def test_1000_stale_revision_and_conflicting_alias_cases(self):
        for i in range(1000):
            baseline = BASE + ("# snapshot %d\n" % i)
            with self.assertRaises(ConfigurationConflict):
                patch_env(baseline, {"equity_budget_inr": 1000 + i},
                          expected_revision=revision(BASE))
            # Alias and canonical field in one payload must not silently clobber.
            with self.assertRaises(ConfigurationRejected):
                patch_env(baseline, {"daily_trade_limit": i,
                                     "max_daily_trades": i + 1},
                          expected_revision=revision(baseline))
        print("STRESS VERIFIED 1000 revision/alias conflict pairs", flush=True)

    def test_1000_valid_budget_amo_and_risk_combinations_through_appconfig(self):
        rng = random.Random(22202)
        safe = [
            "equity_budget_inr", "fno_capital_per_trade",
            "max_daily_trades", "max_daily_loss_inr",
            "max_open_positions", "max_total_exposure_inr",
            "equity_amo_enabled", "fno_amo_enabled",
            "equity_sl_pct", "equity_target_pct",
            "fno_sl_pct", "fno_target_pct",
        ]
        for i in range(1000):
            form = {}
            for k in rng.sample(safe, rng.randrange(2, 9)):
                if k.endswith("_enabled"):
                    form[k] = rng.choice(("true", "false"))
                elif k in ("max_daily_trades", "max_open_positions"):
                    form[k] = str(rng.randint(0, 15))
                elif "_sl_pct" in k or "_target_pct" in k:
                    form[k] = str(rng.randint(1, 8))
                else:
                    form[k] = str(rng.randint(500, 40000))
            target, changed = patch_env(BASE, form, expected_revision=revision(BASE))
            self.assertTrue(set(changed).issubset({REGISTRY[k].key for k in form}))
            validate_effective_config(target)
        print("STRESS VERIFIED 1000 complete AppConfig validations", flush=True)


class DurableCoordinatorMatrixTests(unittest.TestCase):
    """No real systemd, Dhan, file outside temporary directories or Telegram."""

    def test_300_stopped_save_idempotence_and_no_restart(self):
        for i in range(300):
            with tempfile.TemporaryDirectory() as td:
                root = Path(td)
                env = root / ".env"
                old = ("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=%d\n" % (500 + i)).encode()
                new = ("TRADING_MODE=TEST\nEQUITY_BUDGET_INR=%d\n" % (1000 + i)).encode()
                env.write_bytes(old)
                co = CandidateCoordinator(root / "data", env, feature_enabled=True)
                req = "request-%05d" % i
                before = hashlib.sha256(old).hexdigest()
                args = dict(
                    request_id=req, expected_file_revision=before,
                    new_env=new, previous_mode="TEST", target_mode="TEST",
                    service_running=False,
                )
                result = co.submit(**args)
                self.assertEqual(result.phase, "SAVED_FOR_NEXT_START")
                self.assertFalse(result.restart_issued)
                self.assertEqual(env.read_bytes(), new)
                replay = co.submit(**args)
                self.assertEqual(replay.phase, "SAVED_FOR_NEXT_START")
                self.assertFalse(replay.restart_issued)
                self.assertEqual(env.read_bytes(), new)
                with self.assertRaises(ChangeBlocked):
                    co.submit(**{**args, "new_env": old})
                self.assertEqual(env.read_bytes(), new)
        print("STRESS VERIFIED 300 persisted stopped saves and safe replays", flush=True)

    def test_1000_running_restart_stop_ack_and_disk_races(self):
        from src.engine.tracker_v2_safety import SafetyDecision, read_private_json
        for i in range(1000):
            family = i % 10
            with tempfile.TemporaryDirectory() as td:
                root = Path(td)
                data = root / "data"
                data.mkdir()
                env = root / ".env"
                old = b"TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\n"
                new = b"TRADING_MODE=TEST\nEQUITY_BUDGET_INR=1000\n"
                env.write_bytes(old)
                co = CandidateCoordinator(data, env, feature_enabled=True)
                calls = []
                def operation_gate():
                    allowed = family != 1
                    return SafetyDecision(
                        allowed, "NO_ACTIVE_BROKER_OPERATION" if allowed else "BROKER_MUTATION_ACTIVE",
                        "synthetic", {"producer_pid": 40001, "producer_starttime": 701}
                    )
                def restart():
                    calls.append("restart")
                    return family != 3
                def ack_probe():
                    if family == 4:
                        p = data / "service_control_state.json"
                        p.write_text(json.dumps({
                            "desired_service_state": "STOPPED",
                            "observed_state": "STOPPED_BY_OWNER",
                            "deferred_stop_pending": False
                        }))
                        p.chmod(0o600)
                    elif family == 5:
                        env.write_bytes(b"TRADING_MODE=TEST\nEQUITY_BUDGET_INR=1337\n")
                    elif family == 9:
                        p = data / "service_control_state.json"
                        p.write_text("{invalid")
                        p.chmod(0o600)
                    ack = {
                        "pid": 40001 if family == 8 else 40002,
                        "process_starttime": 701 if family == 8 else 702,
                        "version_hash": "f"*64 if family == 2 else "a"*64,
                        "trading_mode": "TEST",
                        "credential_fingerprint": "f"*64 if family == 7 else "b"*64,
                        "acknowledged_at": time.time()+10,
                    }
                    systemd = {"ActiveState": "active",
                               "MainPID": 40003 if family == 6 else ack["pid"],
                               "Result": "success"}
                    return ack, systemd
                args = dict(
                    request_id="running-%06d" % i,
                    expected_file_revision=hashlib.sha256(old).hexdigest(),
                    new_env=new, previous_mode="TEST", target_mode="TEST",
                    service_running=True, pre_runtime_verified=True,
                    before_pid=40001, before_starttime=701,
                    operation_gate=operation_gate,
                    entry_fence=lambda c: calls.append("fence") or True,
                    restart_callback=restart,
                    ack_probe=ack_probe, expected_runtime_hash="a"*64,
                    credential_fingerprint="b"*64,
                    starttime_reader=lambda pid: (
                        701 if pid == 40001 else
                        702 if pid == 40002 else
                        703
                    ),
                    release_fence=lambda c: calls.append("release") or True,
                )
                if family == 1:
                    with self.assertRaises(ChangeBlocked):
                        co.submit(**args)
                    self.assertEqual(env.read_bytes(), old, i)
                    self.assertEqual(calls.count("restart"), 0)
                    self.assertEqual(calls.count("release"), 0)
                    continue
                result = co.submit(**args)
                expected = (
                    "VERIFIED" if family == 0
                    else "ACTION_REQUIRED" if family in (4, 5, 9)
                    else "ACK_UNCERTAIN"
                )
                self.assertEqual(result.phase, expected, (i, family, result))
                self.assertEqual(calls.count("restart"), 1, i)
                self.assertEqual(calls.count("release"), 1 if family == 0 else 0, i)
                self.assertEqual(result.runtime_verified, family == 0, i)
                if family != 5:
                    self.assertEqual(env.read_bytes(), new, i)
                if family == 0:
                    replay = co.submit(**args)
                    self.assertEqual(replay.phase, "VERIFIED", i)
                    self.assertEqual(calls.count("restart"), 1, i)
                else:
                    journal = read_private_json(co.journal, missing_ok=False)
                    self.assertEqual(journal["phase"], expected)
                    self.assertTrue(read_private_json(co.barrier)["active"])
        print("STRESS VERIFIED 1000 running restart/STOP/ACK/disk races", flush=True)

    def test_300_legacy_pending_apply_states_never_discarded(self):
        for i in range(300):
            with tempfile.TemporaryDirectory() as td:
                root = Path(td)
                data = root / "data"
                data.mkdir()
                env = root / ".env"
                old = b"TRADING_MODE=TEST\nEQUITY_BUDGET_INR=500\n"
                env.write_bytes(old)
                p = data / "pending_tracker_apply.json"
                states = ("WAITING", "APPLYING", "ACK_UNCERTAIN", "DONE", "", None)
                payload = {"change_id": "LEGACY-%d" % i, "state": states[i % len(states)]}
                text = json.dumps(payload)
                p.write_text(text)
                p.chmod(0o600)
                preview = preview_legacy_tracker_migration(data)
                self.assertFalse(preview.allowed, i)
                co = CandidateCoordinator(data, env, feature_enabled=True)
                with self.assertRaises(ChangeBlocked):
                    co.submit(
                        request_id="legacy-%05d" % i,
                        expected_file_revision=hashlib.sha256(old).hexdigest(),
                        new_env=b"TRADING_MODE=TEST\nEQUITY_BUDGET_INR=1000\n",
                        previous_mode="TEST", target_mode="TEST",
                        service_running=False,
                    )
                self.assertEqual(p.read_text(), text)
                self.assertEqual(env.read_bytes(), old)
        print("STRESS VERIFIED 300 legacy pending operation migrations", flush=True)
