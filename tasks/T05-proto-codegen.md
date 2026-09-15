---
id: T05
title: Generate TS protobuf/gRPC code from cadence IDLs
epic: cadence-typescript-client
depends_on: [T01]
status: pending
---
# T05 — Proto codegen

**Given** `idls/proto/api/v1/*.proto` from the cadence IDL submodule
**When** ts-proto codegen runs under nix-provided protoc
**Then** `src/generated/` contains typed service stubs usable with @grpc/grpc-js against the server's gRPC port 7833.
