import { Effect, Schema, Stream } from 'effect'
import type { EventEnvelope } from '@hermes-webhook-relay/envelope'

export class Unauthorized extends Schema.TaggedError<Unauthorized>()('WebhookUnauthorized', {}) {}
export class InvalidPayload extends Schema.TaggedError<InvalidPayload>()('WebhookInvalidPayload', {}) {}
export class BodyTooLarge extends Schema.TaggedError<BodyTooLarge>()('WebhookBodyTooLarge', {}) {}
export class IngestionFailed extends Schema.TaggedError<IngestionFailed>()('WebhookIngestionFailed', {}) {}
// Resolution errors: the provider is unknown or not enabled, or is enabled but
// misconfigured. Never attach configuration values to these.
export class ProviderNotFound extends Schema.TaggedError<ProviderNotFound>()('WebhookProviderNotFound', {}) {}
export class ConfigurationError extends Schema.TaggedError<ConfigurationError>()('WebhookConfigurationError', {}) {}

export type IngestionError = Unauthorized | InvalidPayload | BodyTooLarge | IngestionFailed

// Shape shared by every provider's ingestion service.
export interface Ingestion {
  readonly ingest: (request: Request) => Effect.Effect<EventEnvelope.EventEnvelope, IngestionError>
}

export const readBody = (request: Request, maxBytes: number) => Effect.suspend(() => {
  const body = request.body
  if (!body) return Effect.succeed(new Uint8Array())
  if (body.locked) return Effect.fail(new InvalidPayload())
  const buffer = new Uint8Array(maxBytes)
  return Stream.fromReadableStream({
    evaluate: () => body,
    onError: () => new InvalidPayload(),
    releaseLockOnEnd: true,
  }).pipe(
    Stream.runFoldEffect(() => 0, (length, chunk) => {
      const nextLength = length + chunk.byteLength
      if (nextLength > maxBytes) return Effect.fail(new BodyTooLarge())
      return Effect.sync(() => {
        buffer.set(chunk, length)
        return nextLength
      })
    }),
    // Stream releases its reader before this finalizer. Cancellation must not
    // block a timeout or replace the original error if the source rejects it.
    Effect.ensuring(Effect.sync(() => { void body.cancel().catch(() => {}) })),
    Effect.map((length) => buffer.slice(0, length)),
  )
})
