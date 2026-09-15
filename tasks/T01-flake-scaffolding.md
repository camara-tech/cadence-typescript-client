---
id: T01
title: Nix flake + TS project scaffolding
epic: cadence-typescript-client
depends_on: []
status: done
---
# T01 — Nix flake + TS project scaffolding

**Given** a bare repo with only LICENSE/README
**When** the flake and package scaffolding are created
**Then** `nix develop` provides node 22, go 1.24, protoc, grpcurl; `npm test` runs vitest with vitest-gwt.
