/**
 * Structured error codes surfaced by the client, mirroring the reference
 * clients' typed errors (shared.EntityNotExistsError etc.).
 */
export type CadenceErrorCode =
  | 'WorkflowExecutionAlreadyStarted'
  | 'EntityNotExists'
  | 'DomainAlreadyExists'
  | 'BadRequest'
  | 'QueryFailed'
  | 'Internal';

export class CadenceError extends Error {
  constructor(
    public readonly code: CadenceErrorCode,
    message: string,
  ) {
    super(message);
    this.name = 'CadenceError';
  }
}

/**
 * Maps a grpc ServiceError onto the api/v1 error taxonomy. The server
 * encodes typed errors as gRPC status codes:
 *  - NOT_FOUND (5)      → EntityNotExists / WorkflowExecutionAlreadyCompleted
 *  - ALREADY_EXISTS (6) → WorkflowExecutionAlreadyStarted | DomainAlreadyExists
 *  - INVALID_ARGUMENT (3) → BadRequest | QueryFailed
 */
export function mapGrpcError(err: { code: number; details?: string; message: string }): CadenceError {
  const message = err.details ?? err.message;
  let code: CadenceErrorCode;
  switch (err.code) {
    case 5: // NOT_FOUND
      code = 'EntityNotExists';
      break;
    case 6: // ALREADY_EXISTS
      code = /domain/i.test(message) ? 'DomainAlreadyExists' : 'WorkflowExecutionAlreadyStarted';
      break;
    case 3: // INVALID_ARGUMENT
      code = /query/.test(message) ? 'QueryFailed' : 'BadRequest';
      break;
    default:
      code = 'Internal';
  }
  return new CadenceError(code, message);
}
