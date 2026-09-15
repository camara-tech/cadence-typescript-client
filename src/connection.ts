import * as grpc from '@grpc/grpc-js';
import { CadenceError, mapGrpcError } from './errors.js';

/** Minimal unary RPC surface matching ts-proto's generated client expectations. */
export interface Rpc {
  request(service: string, method: string, data: Uint8Array): Promise<Uint8Array>;
}

export interface ConnectionOptions {
  /** Cadence frontend gRPC address. Defaults to 127.0.0.1:7833. */
  address?: string;
  /** Identity recorded on task polls and start requests. */
  identity?: string;
}

const API_PACKAGE = 'uber.cadence.api.v1';
const API_SERVICE = 'cadence-frontend';
const DEFAULT_TIMEOUT_MS = 60_000;
const LONG_POLL_TIMEOUT_MS = 90_000;

/**
 * A gRPC connection to the Cadence frontend speaking the api/v1 protobuf
 * services. `request` matches the ts-proto generated `Rpc` interface so the
 * generated clients can be wired directly onto it.
 */
export class Connection implements Rpc {
  readonly identity: string;
  private readonly client: grpc.Client;

  private constructor(client: grpc.Client, identity: string) {
    this.client = client;
    this.identity = identity;
  }

  static connect(opts: ConnectionOptions = {}): Connection {
    const address = opts.address ?? '127.0.0.1:7833';
    const client = new grpc.Client(address, grpc.credentials.createInsecure());
    return new Connection(client, opts.identity ?? defaultIdentity());
  }

  request(service: string, method: string, data: Uint8Array): Promise<Uint8Array> {
    // Long-poll calls block server-side until a task is available.
    const timeoutMs = method.startsWith('PollFor') || method === 'GetWorkflowExecutionHistory'
      ? LONG_POLL_TIMEOUT_MS
      : DEFAULT_TIMEOUT_MS;
    return new Promise((resolve, reject) => {
      const path = `/${service}/${method}`;
      const metadata = new grpc.Metadata();
      metadata.set('rpc-caller', this.identity);
      metadata.set('rpc-service', API_SERVICE);
      metadata.set('rpc-encoding', 'proto');
      const deadline = new Date(Date.now() + timeoutMs);
      this.client.makeUnaryRequest<Uint8Array, Uint8Array>(
        path,
        (arg) => Buffer.from(arg),
        (arg) => new Uint8Array(arg),
        Buffer.from(data),
        metadata,
        { deadline },
        (err: grpc.ServiceError | null, response?: Uint8Array) => {
          if (err) {
            reject(mapGrpcError(err));
            return;
          }
          resolve(response ?? new Uint8Array());
        },
      );
    });
  }

  close(): void {
    this.client.close();
  }
}

function defaultIdentity(): string {
  return `cadence-ts-client-${process.pid}`;
}
