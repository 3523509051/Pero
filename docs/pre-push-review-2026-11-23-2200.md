# Publication review: memory convergence and SDK completion

Date: 2026-11-23, Asia/Shanghai. Verdict: PASS for reviewed source publication.
Base: origin/main (862b3dc). Candidate: codex/convergence-release-20261123,
one fast-forward child of the public baseline; legacy development history is excluded.

## Plan adherence

Publishes the reviewed 50/20/8000 interaction-memory policy, bounded bystander
context, deterministic retrieval, archive-derived facts/events, Beijing time,
usage reporting and SDK completion/reset boundary. README and engineering diagrams
describe actual defaults and distinguish ordinary I/O recovery from crash atomicity.

## Code quality

Memory and SDK paths were reviewed through R03, R06 and R06-R01 acceptance.
Sources retain stable message identities, scoped resources and sent-only assistant
history. Plugin results remain temporary references rather than system instructions.
No additional reviewer model or automatic model retry is introduced.

## Commit hygiene

The publication helper starts from public main using an alternate index. It scans
the complete resulting tree for known credentials and common credential patterns,
checks relative documentation links and whitespace, and excludes runtime data.
Release manifests identify the exact candidate tree; no production config is published.

## Validation

Frontend TypeScript/Vite production build passed. Targeted memory R03 and SDK R06
checks are rerun during release preparation; earlier accepted scope includes
R06-R01 reset races, image mounting and archived-turn completion. Bilibili cache
fault probes and 25 offline checks pass in its independent repository.

## Operational boundaries

Actual model quality and WeChat delivery remain user acceptance checks. Deployment
must keep credentials and explicit conversation overrides, back up the stopped-Core
database and rehearse migration. The matching Docker worker image and installed
plugin are verified separately; a source publication is not a production-health claim.
