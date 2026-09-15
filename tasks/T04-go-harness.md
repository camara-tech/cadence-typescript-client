---
id: T04
title: Go adapter harness validated green against real server
epic: cadence-typescript-client
depends_on: [T03]
status: done
---
# T04 — Go adapter validation

**Given** the GWT suite and a running Cadence server (gRPC :7833)
**When** the Go harness (cadence-go-client + yarpc gRPC transport) implements the port and DRIVER=go
**Then** the full suite passes, proving the tests encode the reference client's contract.
