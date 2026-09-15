# Cadence Client — DDD Domain Model

Derived from `cadence-go-client` (behavioral spec) and `cadence-python-client`, viewed through a
Domain-Driven Design lens. This document drives the black-box GWT conformance suite in `tests/gwt/`.

## Strategic Classification

| Subdomain | Classification | Rationale |
|---|---|---|
| Workflow execution driving (start/signal/query/cancel/terminate/result) | **Core** | The reason the SDK exists; competitive differentiation |
| Worker hosting (task polling, workflow/activity execution) | **Core** | Executes the domain logic; determinism is the hard problem |
| Data encoding (DataConverter, Payload metadata) | Supporting | Required by both core contexts; pluggable |
| Interception (client/workflow interceptors, propagators) | Supporting | Cross-cutting extension point |
| Retry / cron / timeout policies | Supporting | Policy value objects interpreted by server + client |
| Domain (namespace) administration | Supporting | Operational CRUD, needed for test bootstrap |
| Visibility (list/count workflows) | Supporting | Read model over executions |
| gRPC transport, protobuf codec, UUIDs | Generic | Commodity infrastructure |

## Bounded Contexts

### 1. Client Kernel
The composition root. Owns connection identity and shared policy.

- **Aggregate:** `Client` — created from `ClientOptions` (host, port, domain/namespace, identity,
  dataConverter, interceptors). All other contexts are reached through it.
- **Value objects:** `ClientOptions`, `Identity` (defaults to `pid@host`).
- **Invariants:** domain must exist before operations succeed; identity is attached to every task
  poll and request for auditability.

### 2. Workflow Execution (core)
- **Aggregate root:** `WorkflowExecution` — identified by `(workflowId, runId)`. `workflowId` is the
  business identity; `runId` identifies one run of it (changes on continue-as-new/retry).
- **Entities:** `WorkflowRun` (awaitable handle), `HistoryEvent` (event-sourced log entry).
- **Value objects:** `StartWorkflowOptions` (id, requestId, taskList, executionStartToCloseTimeout,
  decisionTaskStartToCloseTimeout, idReusePolicy, retryPolicy, cronSchedule, memo, searchAttributes,
  delayStart), `RetryPolicy`, `WorkflowIDReusePolicy` (AllowDuplicateFailedOnly | AllowDuplicate |
  RejectDuplicate | TerminateIfRunning), `ParentClosePolicy`, `Memo`, `SearchAttributes`.
- **Domain commands:** Start, Signal, SignalWithStart, Query, Cancel, Terminate.
- **Invariants:**
  - At most one open execution per `workflowId` (per reuse policy).
  - Start with same requestId is idempotent (returns same run).
  - Cancel is cooperative (workflow observes cancellation); Terminate is forceful.
  - Queries are read-only and never mutate history.
- **Domain errors:** `WorkflowExecutionAlreadyStartedError`, `EntityNotExistsError`,
  `CancellationAlreadyRequestedError`, `QueryFailedError`, `AccessDeniedError`, `DomainNotActiveError`.
- **Lifecycle (completed):** `WorkflowExecutionCompleted` / `Failed` / `Canceled` / `TimedOut` /
  `Terminated` / `ContinuedAsNew` — observable via result-await, describe, and history.

### 3. Worker (core)
- **Aggregate:** `Worker` — bound to `(domain, taskList)`.
- **Entities:** registrations (`WorkflowDefinition`, `ActivityDefinition`) keyed by name.
- **Domain behavior:**
  - Poll decision tasks → replay history → execute user workflow function deterministically →
  respond with commands (ScheduleActivity, StartChildWorkflow, StartTimer, CompleteWorkflow,
  ContinueAsNew, RespondToQuery...).
  - Poll activity tasks → invoke activity function → respond (completed/failed) → heartbeat for
  long activities.
- **Invariants:** workflow code must be deterministic under replay; same history → same commands.
- **Value objects:** `WorkerOptions` (maxConcurrentActivityExecutionSize, etc.),
  `ActivityOptions` (scheduleToCloseTimeout, startToCloseTimeout, heartbeatTimeout, retryPolicy,
  taskList override), `ActivityInfo` (taskToken, workflowExecution, attempt, heartbeatDetails).

### 4. Data Encoding
- **Value objects:** `Payload { metadata: {encoding, ...}, data: bytes }`.
- **Domain service:** `DataConverter` — `toPayloads(args) -> Payloads`, `fromPayloads<T>(payloads)`.
  Default: JSON with `encoding=json/plain`; pass-through for `byte[]` (`encoding=binary/plain`).
- `EncodedValue` lazy-reads a payload (Go `encoded.Value` / Python `EncodedValue`).

### 5. Interception
- **WorkflowInterceptor:** observes/overrides workflow-side calls (executeActivity, executeChildWorkflow,
  signalExternalWorkflow, sideEffect, getVersion, ...).
- **RPC interceptors:** chain of unary interceptors on the gRPC client.
- Observable effect for black-box tests: invocation counting / header mutation visible to handlers.

### 6. Domain Administration
- **Aggregate:** `Domain` (name, status, retention, emitMetric, description, ownerEmail).
- **Commands:** Register, Describe, Update, Failover. Errors: `DomainAlreadyExistsError`,
  `EntityNotExistsError`, `BadRequestError`.

### 7. Visibility
- Read model over executions: `ListOpenWorkflowExecutions`, `ListClosedWorkflowExecutions`
  (filter by execution time range / workflow type / status), `CountWorkflowExecutions`
  (requires ElasticSearch on the server).

## Context Map

```
            ┌───────────────────────────────┐
            │        Client Kernel          │
            │  (facade, identity, options)  │
            └──────┬─────────────┬──────────┘
        conforms   │             │  conforms
   ┌───────────────▼──┐   ┌──────▼────────────┐
   │ Workflow         │   │ Worker            │
   │ Execution        │◄──┤ (hosts handlers,  │
   │ (core aggregate) │   │  replays history) │
   └───────┬──────────┘   └──────┬────────────┘
           │      shared kernel  │
   ┌───────▼──────────┐   ┌──────▼────────────┐
   │ Data Encoding    │   │ Interception      │
   │ (DataConverter)  │   │ (interceptors)    │
   └──────────────────┘   └───────────────────┘
           │                      │
   ┌───────▼──────────────────────▼──────┐
   │  Service Gateway (ACL): generated   │
   │  protobuf/gRPC client (api/v1 IDL)  │
   └─────────────────────────────────────┘
```

- The generated gRPC API (`cadence.workflowservice.v1` / thrift `WorkflowService`) is an
  **anti-corruption layer**: domain code never touches raw IDL types.
- Both reference clients expose the same ubiquitous language (StartWorkflow, SignalWorkflow,
  QueryWorkflow, CancelWorkflow, TerminateWorkflow, CompleteActivity, RecordActivityHeartbeat,
  RegisterDomain, ...), which is the contract the GWT suite encodes.

## Test Architecture (hexagonal)

The GWT suite (vitest + vitest-gwt) tests the **port** `CadenceTestDriver`, never an SDK directly:

```
tests/gwt/*.test.ts ──► CadenceTestDriver (port)
                            ▲            ▲
              GoDriver      │            │  TsDriver
        (HTTP → Go harness  │            │  (in-process TS client)
         → cadence-go-client│
         → real server)     │
                            └──► Cadence server (gRPC 7833)
```

- `DRIVER=go` proves the suite encodes the real client contract (green against the reference).
- `DRIVER=ts` is the red-green TDD loop for this repo's TypeScript client.
- Shared **behavior catalog** (workflow/activity implementations) is implemented natively by each
  adapter so tests stay language-agnostic.
