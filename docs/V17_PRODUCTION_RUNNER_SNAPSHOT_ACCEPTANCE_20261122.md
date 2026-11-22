# V17 production runner — runtime snapshot repair (review candidate)

## Source of the defect

- `deploy/install_vm_services.sh` configures `trading_bot.service` with `ExecStart=$REPO_DIR/venv/bin/python3 run.py`.
- V17's initial runtime snapshot task was added only to `src/main.py`'s `start_telegram_listener()` path, **not** the actual `run.py` production runner.
- The Telegram startup card is emitted from `run.py` via `src.cli.format_startup_online_card` using startup reconciliation and journal counts, rather than the WebUI's process-bound, continuously refreshed runtime snapshot.
- The production VM observation before this fix was `runtime_evidence_at: None` and no `runtime_operational_snapshot.json` in its configured data directory after restart. This indicates missing WebUI evidence, not proof that protection/reconciliation failed.
- The `systemctl` live unit `ExecStart` must still be independently checked at deployment time; the repository installer path alone is not a substitute for live unit inspection.

## Narrow change

- `run.py` starts the same existing `publish_operational_snapshot` writer used by `src/main.py`, after the watcher task starts and after Telegram startup. It uses the actual Telegram client's `is_connected()` state.
- The writer retries every ten seconds and catches `Exception`, logging only the exception type. It does not make broker calls, change trading gates, submit orders, or override saved Owner STOP state.
- The writer task is cancelled during the existing shutdown path. It is not allowed to keep the service alive by itself.
- Production runner test is located in `tests/` and now correctly opens the repository-root `run.py` (the earlier offline ZIP accidentally pointed to `tests/run.py` and its three packaged tests could not run in that layout). This error was found and repaired during independent pre-PR review.

## Exact offline verification

- `python -m unittest discover -s tests -p 'test_v17_production_snapshot_runner.py' -v`: 4 tests PASS.
- `python -m py_compile run.py tests/test_v17_production_snapshot_runner.py`: PASS.
- Reverse patch reconstructs base GitHub `run.py` blob `790103bc5ca40f3f84fd1f9a06aa4c63fc5eda56` from commit `324de0dd3537d9da8aa9d4580019003286e50c27`; reapplying patch exactly reproduces candidate `run.py` bytes.
- Full repository CI and any production runtime acceptance remain required; no Dhan/Telegram/live account calls were made for local tests.

## Deployment and safety gates

1. Review diff and GitHub CI on the precise PR head before any merge; merge requires user approval.
2. **Do not deploy to a live VM or restart `trading_bot.service` under this PR approval.** Trading-process restart requires separate explicit authorization after reconciliation, open-order and protection-watcher checks.
3. Verify the live systemd `ExecStart`, installed GitHub SHA, `.env` preservation, owner-control intent, service coordination and rollback before any future deployment.
4. After separately approved restart, confirm runtime snapshot exists, has private permissions, matches current trading PID/starttime and account fingerprint, remains fresh, and is accepted by `read_verified_snapshot`.
5. Preserve `UNVERIFIED` when snapshot is absent/stale and preserve the observed Owner `STARTING` versus `RUNNING` warning until separately diagnosed. Do not mark any status healthy from Telegram wording alone.
6. Separate known issue: `format_startup_online_card()` may overstate watcher verification based on startup reconciliation/journal counts. This patch does **not** change its message semantics. Review that claim in a dedicated correction.
7. Separate known issue: coordinator can leave `observed_state=STARTING` if post-restart verification times out after systemd already started the process. Do not suppress it or bypass restart safety gates.
