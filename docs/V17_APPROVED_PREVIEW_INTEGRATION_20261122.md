# V17 — Approved six-screen HTML visual integration

Review date: 22 November 2026. Base: `main` at `6144532d832cc046500769f49b190454b6ffdd03` (PR #119 already merged).

## Reference and scope

Source of visual direction: the original `uditbot_v17_design_preview_mobile_fixed.html` supplied in the project, the user-provided 2026-11-22 six-screen mobile screenshots, and the text inventory of Dashboard, Control, Setup, Telegram, AMO, Health. The reference is **illustrative only**, with no live data or working broker/service controls. This change copies the structure and visual treatments, not its simulated claims.

The implementation preserves the **six bottom tabs on iPhone**, simple top header with theme/account access, dashboard service/mode/reconciliation/protection/entry-gate cards, quick actions, four Setup landing categories, view-specific Trading Controls, native Telegram Designer, AMO Settings and Health checks. Desktop uses the same design language with a six-tab top navigation, wider cards and grids. The existing authenticated native pages, original 54 unique named Setup controls, CSRF, native Save & Apply, service-control and AMO routes are retained.

Dashboard **never asserts** Dhan connected, 20:00 token expiry, HEALTHY reconciliation, protection, or OPEN entry gate from saved settings or preview text. Unsupported evidence is labelled `UNVERIFIED`. An Owner service-intent WARN remains visible. Restart uses the existing CSRF-protected service-control endpoint, requires an explicit confirmation, and is disabled until independent owner-intent PASS and verified running/transition gates. No new trading or broker backend calls.

## Verification (synthetic offline fixtures; not production acceptance)

- Independent diff against exact current GitHub `main`: only `src/control_center_ui.py`, `src/premium_ui.py`, `src/premium_ui.css` and `src/premium_ui.js` changed. **66 other source files unchanged**, including all trading, broker, reconciliation, risk, notifier and native Setup backend modules.
- Browser: 24 captures (six views × desktop/iPhone × dark/light), no horizontal overflow, JS errors or mutation requests; 54 unique Setup names.
- Restart/owner WARN, account drawer, bad-status fail closed, responsive layouts and safe long names: 18/18 browser interaction checks.
- Control Center response fail-closed behaviors: 10/10 simulated-browser scenarios.
- Existing and new focused tests: 48 PASS + 106 subtests in the latest re-run; earlier expanded review 118 PASS + 221 subtests.
- Independent baseline source check: PASS, including the exact main blob SHA of all five previously committed source/test files.

## Release gate

This is a **presentation-only change**. Owner has waived strict pixel SSIM matching; exact representative state/color fidelity cannot be claimed when approved screenshots include illustrative status data and browser chrome. GitHub PR regression must pass against the new commit before merge. Do not confuse a GitHub merge with VM deployment. VM update requires inspecting current HEAD/clean worktree and restarting **only** the WebUI service safely; no restart or broker action was performed by this review.
