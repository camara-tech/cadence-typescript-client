# cadence-typescript-client

Unofficial [Cadence](https://cadenceworkflow.io/) TypeScript client.

## Development

The environment is a nix flake (loaded via direnv): Node 22, Go 1.24, protoc, grpcurl.

```bash
npm install
npm test          # GWT black-box suite (DRIVER=go by default)
npm run typecheck
npm run codegen   # regenerate src/generated from the cadence IDLs
```

## Testing

The GWT suite (`vitest` + `vitest-gwt`) black-box tests a `CadenceTestDriver`
port against a **real Cadence server** (gRPC on `127.0.0.1:7833`):

```bash
# against the reference Go client (validates the suite's contract)
DRIVER=go npm test

# against this repo's TypeScript client (the TDD target)
DRIVER=ts npm test
```

The Go harness lives in `tests/harness/go-driver`; start it with
`SERVICE_ADDR=127.0.0.1:7833 tests/harness/go-driver/run.sh` (builds and runs
the HTTP bridge). A local server can be started with
`docker compose -f dev/docker-compose.yml up`.

See `docs/domain-model.md` for the DDD analysis that drives the suite, and
`tasks/` for the task breakdown.

## Usage

```ts
import { Connection, WorkflowClient, Worker, JsonDataConverter } from 'cadence-typescript-client';

const connection = Connection.connect({ address: '127.0.0.1:7833' });
const client = new WorkflowClient(connection, { dataConverter: new JsonDataConverter() });

await client.ensureDomain('my-domain');

const run = await client.startWorkflow('greeting', {
  taskList: 'my-task-list',
  executionStartToCloseTimeoutMs: 60_000,
}, 'my-domain', 'world');

const result = await client.waitForClose('my-domain', run.workflowId, run.runId);

const worker = new Worker(connection, 'my-domain', 'my-task-list', new JsonDataConverter());
worker.registerWorkflow('greeting', async (ctx, name) => {
  await ctx.runActivity('compose-greeting', name);
});
worker.registerActivity('compose-greeting', async (name) => `Hello, ${name}!`);
worker.start();
```
