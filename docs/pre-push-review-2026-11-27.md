# Pre-push review — 2026-11-27

## Independent installer ownership

PASS. Added install-pero.cmd to the Pero root, resolving paths relative to itself and installing only this project's backend and frontend. webot owns the combined installer and calls this entry. Incomplete repository validation and delegation/failure paths were checked with isolated shell fixtures; no live services or dependencies were changed by verification.

## All-group documentation revision

PASS. README and the combination redirect now describe automatic all-group startup in webot rather than its removed setup/selection step. Membership is synchronized on restart; Pero's code and independent service behavior are unchanged in this documentation-only follow-up.

Verdict: PASS for the local Windows deployment/ownership change.
Base branch: origin/main (69d42daf1297a98a691593f0030c969167e865fc).
Reviewed range: 69d42daf1297a98a691593f0030c969167e865fc..c6ce764e177f863e32670b9e56030770fb339f64. The following report commit contains documentation only.

## 1. Context and scope

User requested webot-owned combination orchestration, independent Pero, explicit WeChat binding, GitHub publication and a fresh checkout for manual installation. Reviewed against the Windows deployment guide and repository boundaries. Release sources were based on current remote main, not a blanket overwrite of a dirty working copy.

## 2. Plan adherence

webot owns preparation, explicit terminal group selection, supervisor, bridge and combined stop/status. Pero retains independent Core/Manager startup; its bridge pairing module, combination command and flags were removed. First selection is in setup.cmd, not a new frontend page. Restarts retain selected groups rather than enabling newly discovered groups.

## 3. Code quality and failure paths

Binding checks explicit account directory/internal identity, PID lifetime, desktop session and window/input ownership. Failed identity is latched. Database fallback is checked before exposing results. Supervisor uses owned Windows jobs and cooperative stop rather than killing process names. New default combination memory/cache is isolated from standalone Pero. Scope/application failures prevent bridge startup.

## 4. Commit hygiene

Reviewed changed text files for credential-shaped literals and oversized artifacts: no findings. No local config, credentials, chat history, binding, virtual environment, node_modules, frontend dist or runtime data was staged. The two removed legacy files were explicitly requested by the ownership migration.

## 5. Verification

- 25 focused offline tests passed: account binding, process/window scoping, sender-boundary rejection, explicit/reused group selection and send receipts.
- Python compilation and git diff whitespace checks passed.
- Real isolated Pero Core and Manager started on loopback test ports and stopped cooperatively.
- webot supervisor started real Pero with a synthetic bridge, observed its connection, then stopped the whole owned set cooperatively. No actual WeChat interaction, chat event or model request was used.
- Existing receipt fixtures were updated to supply the verified-target resolver expected by the current client; no-retry and delivery-result assertions remain intact.

## 6. Limitations and verdict

PASS for publication and clean installation simulation. Real WeChat login, process database-handle visibility, binding-code observation and dual-account/media sending still require the user's interactive desktop test. Do not describe those physical paths as already verified. No frontend redesign or new group-picker page was implemented. The fresh checkout is intentionally left without installed dependencies, configuration or running services.
