---
id: T06
title: TS client kernel + WorkflowClient (red-green)
epic: cadence-typescript-client
depends_on: [T04, T05]
status: pending
---
# T06 — Client kernel + WorkflowClient

**Given** the GWT suite failing under DRIVER=ts (red)
**When** the client kernel (connection, DataConverter, error mapping) and WorkflowClient (start, signal, signal-with-start, query, cancel, terminate, result-await, describe, history) are implemented
**Then** lifecycle/signal/query/cancel/terminate GWT scenarios pass under DRIVER=ts.
