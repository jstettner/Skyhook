import { Clock, Effect, Redacted, Result, Schema } from 'effect'
import * as Base64 from 'effect/encoding/Base64'
import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import * as Webhook from '../Webhook/Webhook'

const MAX_BODY_BYTES = 64 * 1024
const Identifier = Schema.String.pipe(Schema.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,512}$/)))
const fields = {
  event_id: Identifier,
  note_id: Schema.String.pipe(Schema.check(Schema.isPattern(/^not_[a-zA-Z0-9]{14}$/))),
  occurred_at: EventEnvelope.SourceTimestamp,
}
// Ignore additive provider fields, but only normalize explicitly selected metadata.
const Payload = Schema.Union([
  Schema.Struct({ ...fields, event_type: Schema.Literals(['note.generated', 'note.access_granted']) }),
  Schema.Struct({
    ...fields,
    event_type: Schema.Literal('note.edited'),
    data: Schema.Struct({ changed_fields: Schema.Tuple([Schema.Literal('summary')]) }),
  }),
]).annotate({ identifier: 'GranolaPayload' })

export const ingestion = (signingSecret: Redacted.Redacted<string>): Effect.Effect<Webhook.Ingestion, Webhook.ConfigurationError> =>
  Effect.gen(function* () {
    // Decode and import once per construction; never attach secrets to errors.
    const key = yield* Effect.tryPromise({
      try: async () => {
        const secret = Redacted.value(signingSecret)
        if (!secret.startsWith('whsec_') || secret.length > 1024) throw new Error('Invalid secret')
        const encoded = secret.slice(6)
        const decoded = Base64.decode(encoded)
        if (Result.isFailure(decoded) || decoded.success.length === 0 ||
          Base64.encode(decoded.success) !== encoded) throw new Error('Invalid secret')
        return crypto.subtle.importKey('raw', decoded.success,
          { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
      },
      catch: () => new Webhook.ConfigurationError(),
    })
    return {
      ingest: Effect.fn('Granola.ingest')(function* (request: Request) {
        const id = request.headers.get('webhook-id')
        const timestamp = request.headers.get('webhook-timestamp')
        const signatures = request.headers.get('webhook-signature')
        if (!id || !/^[A-Za-z0-9_-]{1,512}$/.test(id) ||
          !timestamp || !/^\d{1,12}$/.test(timestamp) ||
          !signatures || signatures.length > 4096) {
          return yield* Effect.fail(new Webhook.Unauthorized())
        }
        const entries = signatures.split(' ')
        if (entries.length > 16) return yield* Effect.fail(new Webhook.Unauthorized())
        const now = yield* Clock.currentTimeMillis
        if (Math.abs(Math.floor(now / 1000) - Number(timestamp)) > 300) {
          return yield* Effect.fail(new Webhook.Unauthorized())
        }
        const candidates = entries.flatMap((entry) => {
          const match = /^v1,([^,]+)$/.exec(entry)
          if (!match) return []
          const decoded = Base64.decode(match[1])
          if (Result.isFailure(decoded) || decoded.success.length !== 32 ||
            Base64.encode(decoded.success) !== match[1]) return []
          return [decoded.success]
        })
        if (!candidates.length) return yield* Effect.fail(new Webhook.Unauthorized())
        const body = yield* Webhook.readBody(request, MAX_BODY_BYTES)
        const prefix = new TextEncoder().encode(`${id}.${timestamp}.`)
        const signed = new Uint8Array(prefix.length + body.length)
        signed.set(prefix)
        signed.set(body, prefix.length)
        const verified = yield* Effect.tryPromise({
          try: async () => {
            for (const signature of candidates) {
              if (await crypto.subtle.verify('HMAC', key, signature, signed)) return true
            }
            return false
          },
          catch: () => new Webhook.IngestionFailed(),
        })
        if (!verified) return yield* Effect.fail(new Webhook.Unauthorized())
        const text = yield* Effect.try({
          try: () => new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(body),
          catch: () => new Webhook.InvalidPayload(),
        })
        const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Payload))(text).pipe(
          Effect.mapError(() => new Webhook.InvalidPayload()),
        )
        if (payload.event_id !== id) return yield* Effect.fail(new Webhook.InvalidPayload())
        return {
          version: 1 as const,
          provider: 'granola' as const,
          eventId: payload.event_id,
          eventType: payload.event_type,
          sourceRecordId: payload.note_id,
          sourceTimestamp: payload.occurred_at,
        }
      }),
    }
  })

