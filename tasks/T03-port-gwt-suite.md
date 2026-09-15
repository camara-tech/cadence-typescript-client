---
id: T03
title: Define CadenceTestDriver port + GWT black-box suite
epic: cadence-typescript-client
depends_on: [T02]
status: done
---
# T03 — Port + GWT suite

**Given** the bounded contexts in `docs/domain-model.md`
**When** the port `CadenceTestDriver` and its types are defined
**Then** GWT tests (vitest-gwt) cover: domain admin, workflow lifecycle, signal/query, cancel/terminate, worker+activity, retry, timeout, cron, visibility, interceptors — all written against the port only.
