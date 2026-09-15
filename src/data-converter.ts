import type { Payload } from './generated/uber/cadence/api/v1/common.js';

/**
 * Converts values to/from Cadence Payloads. The api/v1 Payload is an opaque
 * byte blob; the default convention (shared with the reference clients) is
 * UTF-8 JSON.
 */
export interface DataConverter {
  toPayload(value: unknown): Payload;
  fromPayload<T = unknown>(payload: Payload): T;
}

export class JsonDataConverter implements DataConverter {
  toPayload(value: unknown): Payload {
    return { data: utf8(JSON.stringify(value ?? null)) };
  }

  fromPayload<T = unknown>(payload: Payload): T {
    return JSON.parse(text(payload.data)) as T;
  }
}

function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function text(b: Uint8Array): string {
  return new TextDecoder().decode(b);
}
