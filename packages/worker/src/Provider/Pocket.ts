import { Clock, Effect, Redacted, Result, Schema } from 'effect'
import * as Hex from 'effect/encoding/Hex'
import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import * as Webhook from '../Webhook/Webhook'

// Pocket sends the full transcript and summaries, so allow long meetings.
const MAX_BODY_BYTES = 2 * 1024 * 1024
const FRESHNESS_MS = 300 * 1000
// Declare only the metadata we forward; excess keys (transcript, summaries,
// user email) are ignored by the decoder and never copied anywhere.
const Payload = Schema.Struct({
  event: Schema.Literals([
    'transcription.completed', 'summary.completed', 'summary.regenerated', 'summary.updated',
    'mind_map.completed', 'action_items.regenerated', 'speakers.labeled',
    'transcript.edited', 'action_items.updated', 'recording.created', 'recording.deleted',
    'recording.merged', 'translation.completed',
  ]),
  timestamp: EventEnvelope.SourceTimestamp,
  recording: Schema.Struct({ id: Schema.String.pipe(Schema.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,512}$/))) }),
}).annotate({ identifier: 'PocketPayload' })

export const ingestion = (signingSecret: Redacted.Redacted<string>): Effect.Effect<Webhook.Ingestion, Webhook.ConfigurationError> =>
  Effect.gen(function* () {
    // Pocket uses the secret string itself as the HMAC key; never attach it to errors.
    const key = yield* Effect.tryPromise({
      try: async () => {
        const secret = Redacted.value(signingSecret)
        if (secret.length > 1024) throw new Error('Invalid secret')
        return crypto.subtle.importKey('raw', new TextEncoder().encode(secret),
          { name: 'HMAC', hash: 'SHA-256' }, false, ['verify'])
      },
      catch: () => new Webhook.ConfigurationError(),
    })
    return {
      ingest: Effect.fn('Pocket.ingest')(function* (request: Request) {
        const timestamp = request.headers.get('x-heypocket-timestamp')
        const signature = request.headers.get('x-heypocket-signature')
        // Missing headers also reject legacy webhooks that were created without a secret.
        if (!timestamp || !/^\d{1,15}$/.test(timestamp) || !signature) {
          return yield* Effect.fail(new Webhook.Unauthorized())
        }
        const now = yield* Clock.currentTimeMillis
        if (Math.abs(now - Number(timestamp)) > FRESHNESS_MS) {
          return yield* Effect.fail(new Webhook.Unauthorized())
        }
        const hex = /^(?:sha256=)?([0-9a-f]{64})$/i.exec(signature)
        const digest = hex && Hex.decode(hex[1].toLowerCase())
        if (!digest || Result.isFailure(digest)) return yield* Effect.fail(new Webhook.Unauthorized())
        const body = yield* Webhook.readBody(request, MAX_BODY_BYTES)
        const prefix = new TextEncoder().encode(`${timestamp}.`)
        const signed = new Uint8Array(prefix.length + body.length)
        signed.set(prefix)
        signed.set(body, prefix.length)
        const verified = yield* Effect.tryPromise({
          try: () => crypto.subtle.verify('HMAC', key, digest.success, signed),
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
        // Pocket sends no event ID. Derive one from signed body fields rather than the
        // delivery header, so a retry that is re-signed with a new header timestamp
        // keeps the same ID as long as Pocket reuses the body.
        const eventId = yield* Effect.tryPromise({
          try: async () => Hex.encode(new Uint8Array(await crypto.subtle.digest('SHA-256',
            new TextEncoder().encode(`${payload.event}\n${payload.recording.id}\n${payload.timestamp}`)))),
          catch: () => new Webhook.IngestionFailed(),
        })
        // Decoding brands the identifiers and checks the envelope's own limits.
        return yield* EventEnvelope.decode({
          version: 1,
          provider: 'pocket',
          eventId,
          eventType: payload.event,
          sourceRecordId: payload.recording.id,
          sourceTimestamp: payload.timestamp,
        }).pipe(Effect.mapError(() => new Webhook.InvalidPayload()))
      }),
    }
  })

