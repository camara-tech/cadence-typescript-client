---
id: T08
title: Interceptors + visibility + final verification
epic: cadence-typescript-client
depends_on: [T07]
status: pending
---
# T08 — Interceptors, visibility, verification

**Given** remaining DRIVER=ts failures
**When** interceptors (workflow + RPC chain) and visibility listing are implemented
**Then** the entire GWT suite is green under DRIVER=ts and DRIVER=go, and `npm test` is the single verification entry point.
