import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { Effect, Result, Schema } from 'effect'
import * as Base64 from 'effect/encoding/Base64'
import * as Inbox from '../Inbox/Inbox'
import * as QueueClient from '../Queue/QueueClient'

const BATCH_SIZE = 50
// Long enough to store and acknowledge a batch; a message whose lease lapses is
// redelivered and then recognized as a duplicate.
const VISIBILITY_TIMEOUT_MS = 60_000
// Bounds one run, so a flooded queue cannot keep a cron script running forever.
const MAX_BATCHES = 20
// How long an event from a provider this consumer doesn't know waits before it is
// offered again. With the queue's max_retries at 100 it outlasts the 24-hour
// retention, leaving time to upgrade the consumer.
export const UNKNOWN_RETRY_DELAY_SECONDS = 3600
// Quarantined bodies are kept for inspection; envelopes are far smaller than this.
const MAX_QUARANTINED_BODY = 64 * 1024

export type Classified =
  | { readonly kind: 'event'; readonly event: EventEnvelope.EventEnvelope }
  // Never decodable by any consumer version: kept in quarantine and acknowledged.
  | { readonly kind: 'malformed'; readonly reason: Inbox.QuarantineReason }
  // An envelope version or provider newer than this consumer: left in the queue.
  | { readonly kind: 'unknown' }

// Only the fields that say which consumer can read the rest.
const Header = Schema.Struct({ version: Schema.Number, provider: Schema.String })

const parseJson = Schema.decodeUnknownResult(Schema.fromJsonString(Schema.Unknown))

// Cloudflare documents `json` bodies as base64 for HTTP pull, but has also served
// them as JSON text; an envelope is always a JSON object, so `{` tells them apart.
const bodyText = (body: string) =>
  body.trimStart().startsWith('{') ? Result.succeed(body) : Base64.decodeString(body.trim())

export const classify = Effect.fn('Pull.classify')(function* (message: QueueClient.Message) {
  const contentType = message.metadata?.['CF-Content-Type']
  if (contentType !== undefined && contentType !== 'json') {
    return { kind: 'malformed', reason: 'unsupported_content_type' } satisfies Classified
  }
  if (typeof message.body !== 'string') return { kind: 'malformed', reason: 'invalid_body' } satisfies Classified
  const text = bodyText(message.body)
  if (Result.isFailure(text)) return { kind: 'malformed', reason: 'invalid_body' } satisfies Classified
  const json = parseJson(text.success)
  if (Result.isFailure(json)) return { kind: 'malformed', reason: 'invalid_json' } satisfies Classified
  const header = Schema.decodeUnknownResult(Header)(json.success)
  if (Result.isSuccess(header) && (header.success.version !== 1 ||
    !(EventEnvelope.Provider.literals as ReadonlyArray<string>).includes(header.success.provider))) {
    return { kind: 'unknown' } satisfies Classified
  }
  const event = yield* EventEnvelope.decode(json.success).pipe(Effect.result)
  if (Result.isFailure(event)) return { kind: 'malformed', reason: 'invalid_envelope' } satisfies Classified
  return { kind: 'event', event: event.success } satisfies Classified
})

export interface Summary {
  readonly received: number
  readonly duplicates: number
  readonly quarantined: number
  // Left in the queue for a newer consumer.
  readonly held: number
}

const bodyForQuarantine = (body: unknown) => {
  const text = typeof body === 'string' ? body : JSON.stringify(body) ?? String(body)
  return text.slice(0, MAX_QUARANTINED_BODY)
}

// Pulls until the queue is drained or MAX_BATCHES is reached. Each batch is stored
// before it is acknowledged; if storing fails nothing in the batch is acknowledged,
// and Cloudflare redelivers it after the visibility timeout.
export const run = Effect.fn('Pull.run')(function* () {
  const queue = yield* QueueClient.QueueClient
  const inbox = yield* Inbox.Inbox
  const summary = { received: 0, duplicates: 0, quarantined: 0, held: 0 }

  for (let batch = 0; batch < MAX_BATCHES; batch++) {
    const messages = yield* queue.pull({ batchSize: BATCH_SIZE, visibilityTimeoutMs: VISIBILITY_TIMEOUT_MS })
    const acks: Array<QueueClient.LeaseId> = []
    const retries: Array<QueueClient.Retry> = []

    for (const message of messages) {
      const classified = yield* classify(message)
      const quarantine = (reason: Inbox.QuarantineReason) =>
        inbox.quarantine({ messageId: message.id, reason, body: bodyForQuarantine(message.body) })
      switch (classified.kind) {
        case 'event': {
          const received = yield* inbox.receive(classified.event)
          if (received === 'Inserted') summary.received++
          else if (received === 'Duplicate') summary.duplicates++
          else {
            yield* quarantine('conflicting_event')
            summary.quarantined++
          }
          acks.push(message.lease_id)
          break
        }
        case 'malformed':
          yield* quarantine(classified.reason)
          summary.quarantined++
          acks.push(message.lease_id)
          break
        case 'unknown':
          retries.push({ leaseId: message.lease_id, delaySeconds: UNKNOWN_RETRY_DELAY_SECONDS })
          summary.held++
          break
      }
    }

    yield* queue.ack(acks, retries)
    if (messages.length < BATCH_SIZE) break
  }
  return summary satisfies Summary
})
