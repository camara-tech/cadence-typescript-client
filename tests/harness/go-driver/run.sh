#!/usr/bin/env bash
# Builds and runs the Go harness (HTTP bridge to cadence-go-client).
set -euo pipefail
cd "$(dirname "$0")"
nix develop -c bash -c 'go build -o /tmp/gwt-driver .'
SERVICE_ADDR="${SERVICE_ADDR:-127.0.0.1:7833}" exec /tmp/gwt-driver
