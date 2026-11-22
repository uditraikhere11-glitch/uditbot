# UditBot V17 — Reference structure restoration and independent acceptance

**Date:** 22 November 2026
**Repository:** `uditraikhere11-glitch/uditbot`
**Authoritative GitHub main SHA:** `03094f97efd00f9184e56ed2dae38f7e175ac366`
**Decision:** **OFFLINE REVIEW CANDIDATE — NO-GO for production**.

## Changes developed

This candidate adds four navigable *views* within the existing authenticated `GET /` page: **Dashboard**, **Trading Controls**, **AMO Orders**, and **System Health**, while preserving the native authenticated `GET /setup` and `GET /setup/telegram-designer` pages. Navigation uses URL fragments; it does not add/rename backend routes or recreate form controls.

The Dashboard's five cards display only source-backed read-only fields: service state, verified running mode, saved Equity AMO, saved F&O AMO, and runtime verification state. No account balance, P&L, fill, broker verification or market status is invented. Status errors clear all cards to **UNVERIFIED**. The existing Start, Stop, Health, Equity AMO, F&O AMO, cancellation and Save & Apply handlers remain the only native controls for their existing workflows. The Health view adds a second button calling the **existing** read-only Health handler; it does not create a route.

The original dark appearance remains the default. A reversible light presentation is available from the same navigation drawer; it stores only a non-sensitive appearance preference. Setup still has exactly 54 unique named fields; its inputs, backend validation, metadata, CSRF and Save & Apply behavior remain unchanged.

## Source immutability audit

From 70 tracked `src/` files, only four presentation files differ:

- `src/control_center_ui.py` — view navigation, read-only Dashboard and Health access, retained status error handling.
- `src/premium_ui.css` — append-only V17 presentation (dark/light; desktop/iPhone), original stylesheet bytes intact as a prefix.
- `src/premium_ui.js` — append-only appearance preference (localStorage contains only `light` or `dark`).
- `src/premium_ui.py` — per-instance UditBot/AmanBot display name and appearance control; preserves native links.

**66 source files unchanged**, including `src/setup_app.py`, `src/control_center_backend.py`, `src/broker`, `src/engine`, `src/notifier`, the trading engine, emergency stop, reconciliation, protection watcher and credential handlers. Critical baseline source blobs were compared with their connected-GitHub SHA-1 blobs (Control Center UI, premium assets, Setup, backend, Dhan and reconciliation).

## Offline test results for this integrated candidate

| Verification | Result |
|---|---|
| Four independently isolated non-root repository test shards | **1,236 passed**, 1,912 additional subtests passed |
| Independent deployment/restore test module | **22 passed**, 6 subtests passed |
| Total | **1,258 passed**, **1,918 additional subtests passed**, zero failures |
| Browser-native view capture | **24 / 24 PASS**: Dashboard, Control, AMO, Health, Setup, Telegram × desktop/iPhone × dark/light |
| Browser page errors | **0** |
| Horizontal overflow | **0 / 24** |
| Outgoing mutation requests from visual capture | **0** |
| Native Setup named field inventory | **54 / 54** in both viewport sizes and both appearances |
| Registry-backed native Setup persistence | **49 / 49** tested via real FastAPI Save & Apply in isolated transactions |
| Previously added Control Center fail-closed browser scenarios | **10 / 10** in prior cycle; should be rerun before production on final full candidate |
| Independent source-policy audit | **PASS**: 4 presentation source changes / 66 protected files unchanged |

**Environment caveat:** Early complete-suite runs failed in root-only deployment script tests and a non-root token test because a previous test run left `/tmp/test_dhan_token.txt` lock/retry files owned by another test account. These synthetic test files were cleared and the tests rerun as non-root with a safe, writable HOME; the final shard passes above are not extrapolated from those failed runs.

## Visual acceptance — still blocked

The original approved artwork is a **1600 × 1410 montage**, combining small, differently framed desktop/mobile screenshots and mixed application states. There are not twelve 1440×900 / 390×844 original independent images of identical framing and data state. The new candidate has 24 actual authenticated native FastAPI GET response captures at fixed viewports with synthetic, explicitly unverified status data. Visual structure and color presentation have been restored further, but **any numerical artwork-to-candidate SSIM of ≥95% would be invalid without a comparable pair and alignment protocol**.

The previous V17 candidate had measured 63.54/100 mean iPhone SSIM against *current GitHub styling*; this is **not** approved-artwork fidelity and has not been carried forward as a release score for the changed design. The current artwork-fidelity gate is **NOT VERIFIED**, not PASS.

## Operational acceptance — still blocked

The real production VM's authenticated systemd control (including owner STOP), Dhan read-only identity/profile, token coordinator, protection watcher, reconciliation, AMO journal, and Telegram live delivery have **not** been exercised here. Browser checks mock external responses; the full repository tests do not prove runtime deployment.

The durable Emergency STOP mechanism exists in the trading engine and CLI, but no separate native authenticated Emergency STOP WebUI endpoint was found. Introducing a high-risk new safety route would violate this visual-only scope. A new route is **not** invented or implied by the UI.

The four-state Control Center layout and all in-page actions are staging functionality, not evidence of real broker order placement, fill, protection or a successful production restart.

## Acceptance decision

| Gate | Status |
|---|---|
| Exact GitHub-main source and immutable trading logic | PASS |
| Existing Setup fields and isolated Save & Apply behavior | PASS |
| Browser view rendering (six logical screens, two viewports, two appearances) | PASS |
| Browser-safe read-only status and Control Center errors | PASS |
| Full offline repository regression | PASS |
| ≥95% against exact approved V0–V9 artwork | **NOT VERIFIED** |
| Real VM/systemd, Dhan, Telegram and protection end-to-end | **NOT VERIFIED** |
| Native Emergency STOP UI | **NOT IMPLEMENTED — original engine/CLI path unchanged** |
| Independent authorization for production | **REJECT / NO-GO** |

**Publication update (22 November 2026):** The independently reviewed 13-file candidate was subsequently published as **draft PR #119** (`review/uditbot-v17-reference-restoration-20261122`). Publication does not change the decision: **NO MERGE, NO PRODUCTION VM DEPLOYMENT, NO LIVE BROKER ACTION**. Never deploy this patch based only on the offline results. Verify exact visual references and live operational acceptance in a staging VM before further authorization.

### Reproduce the offline checks

```bash
AUDITBOT_TEST_ISOLATION=1 PYTHONPATH=. python -m pytest -q -p no:cacheprovider tests/test_v17_native_49_persistence.py tests/test_v17_workspace_contract.py tests/test_v17_personalized_presentation.py tests/test_control_center_ui_contract.py
AUDITBOT_TEST_ISOLATION=1 PYTHONPATH=. V17_CHROMIUM=/usr/bin/chromium python tools/v17_workspace_acceptance.py
python tools/v17_independent_source_audit.py /path/to/verified/main /path/to/v17-candidate
```

The browser harness additionally requires Playwright and Chromium. Do not run the mocked harness against an active production `data/` directory or live `.env`.
