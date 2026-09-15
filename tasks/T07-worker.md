---
id: T07
title: TS Worker — decision + activity task loops (red-green)
epic: cadence-typescript-client
depends_on: [T06]
status: pending
---
# T07 — Worker

**Given** DRIVER=ts failing worker scenarios
**When** the Worker (registry, decision-task poll/replay/execute/respond, activity-task poll/invoke/heartbeat/complete) is implemented
**Then** worker+activity, retry, timeout, and cron GWT scenarios pass under DRIVER=ts.
