# UditBot V17 — Setup navigation and Dhan header correction

**Baseline:** `main` `96db0c616e3a31a790fb09ae35a55b202ed585d8`

## Presentation-only changes

- Setup **no longer duplicates** Bot Service, Trading Mode, Price Source, Settings Status or Emergency STOP status cards. Those operational readouts belong to Dashboard, Trading Control and Health.
- The shared authenticated top header displays the **masked saved Dhan client ID**, or `Not configured`. This is NOT evidence of broker authentication or connectivity. It never renders the complete ID in the header. This does not change any configured credential.
- Six native category cards now correspond one-to-one with the six existing Setup form sections:
  1. Trading Mode & Segments (`#setup-execution`)
  2. Signal & Telegram (`#setup-telegram`)
  3. Dhan Credentials (`#setup-broker`)
  4. Sizing & Pricing (`#setup-sizing`)
  5. Risk & Limits (`#setup-risk`)
  6. Protection & Timing (`#setup-protection`)
- The AMO card is **not** duplicated as a Setup category. A separate small link opens the existing authenticated AMO page (`/#amo`). No AMO toggle or order workflow is changed.
- Clicking a category shows the real matching section, with the other native sections visually hidden; the **same single HTML form** remains authoritative. The user can switch tabs, choose "View all settings", or return to category overview. The original LIVE authorization checkbox remains on Trading Mode and View All, and is only visually hidden when editing unrelated categories. No controls are cloned or mirrored.
- All 54 unique named form fields, including CSRF and revision metadata, the existing Live confirmation and original Save & Apply operation remain unchanged in the native template. The same form still submits its saved values, including those in non-visible sections. Existing backend validation and service-restart safety gates remain untouched.

## Tested

- Local native page/browser captures: 390 × 844 and 1440 × 900, six categories each, single Save & Apply button, no horizontal overflow, no JavaScript errors and no POST/mutation calls.
- Native Setup field identity: 54/54.
- Focused in-repository UI/form regression + true Save & Apply route tests pass.
- The complete isolated local unittest discovery did **not** finish before environment timeout; the next acceptance step is GitHub Actions full regression on the exact PR head. Do not claim a full-suite local pass.

## No backend strategy modifications

No modifications to trading execution, broker authentication, risk rules, reconciliation, protection watcher, AMO route logic, Telegram delivery, the Setup save transaction, or live service state. `src/setup_app.py` removes duplicate read-only status rendering and its associated redundant status inspection; it retains the native form, route handlers and configuration semantics.

## Deployment

Source-only review first. Do not restart the trading service. If GitHub checks pass and merge is approved, update only the WebUI code on the intended VM with a separate verified SSH operation; preserve `.env` and trading state. Review the currently saved Owner STOP/service-intent warning in Dashboard and Health independently; removing a duplicate Setup card does **not** clear that warning or its safety gate.
