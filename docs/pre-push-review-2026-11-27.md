# Pre-push review — 2026-11-27

## Cookie-session CSRF save repair

PASS. Replaced the module-global cached CSRF request path with a shared HTTP client that reads the current authenticated cookie session before each mutation, excluding initial login. This covers cookie changes across tabs and missing/stale in-memory CSRF state. Read requests are unchanged; writes are never automatically retried. Authentication/CSRF server protections remain intact. Verified mocked client scenarios for per-write refresh, session change, read/login behavior and refusal to write after expired/missing session tokens. An isolated FastAPI test reproduced rejection of the stale token before its handler and acceptance of the refreshed token exactly once. TypeScript and Vite production build passed. No real API Key or vault content was read, changed or logged; no model request was sent.

## Independent background entry (latest requirement)

PASS. Pero/start.cmd now launches its existing owned Core/Manager supervisor in the background, with health checks before opening the browser; it never starts webot. stop/status retain independent ownership. Pero owns separate management and OneBot credentials, enabling an explicitly configured external bridge to run in its own process environment. Optional local scope labels are read from Pero's manager state. Verified actual isolated background startup, access-code authentication, cooperative stop, and that stopping Pero does not stop an independently running synthetic bridge. Tests ended with all fixture processes stopped. Existing running legacy combination processes must be stopped through the migration-only old-stop entry before adopting the new startup flow.

## Pero-owned management access code

PASS. Added pero.manager.local_access as the single owner of creation, DPAPI storage, reuse and legacy migration. Both launchers call this module; explicit viewing remains only in Pero/login-code.cmd. Canonical storage is Pero/data/manager/management-token.dpapi. Tested no-create-on-read, encrypted storage, reuse, legacy migration and explicit environment override in temporary stores. Also verified that Pero's viewing CLI produces the access code accepted by the protected management API of a webot-started isolated combination; no credential value was logged. Independent and combined start/stop checks passed. Existing running processes require a normal restart to adopt the unified store.

## Missing frontend source repair

PASS. Restored frontend/src/time.ts, omitted from the earlier source publication despite imports by seven frontend modules. Compared the complete local frontend/src TypeScript/CSS/JSON file set against the release: time.ts was the only absent source. Executed npm ci followed by npm run build against the release checkout; TypeScript checking and Vite production bundling completed successfully. This is a source restoration; no dependency or runtime configuration change. Earlier validation using existing compiled assets did not establish clean source buildability and is superseded by this fresh build verification.

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
