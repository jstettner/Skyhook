import { Config, Context, Effect, Layer, Option, Redacted, Schedule, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'

// The queue's HTTP pull consumer. The token needs Queues Edit (read and write):
// acknowledging a message writes queue state.
export const config = Config.all({
  accountId: Config.String('CLOUDFLARE_ACCOUNT_ID'),
  queueId: Config.String('CLOUDFLARE_QUEUE_ID'),
  token: Config.Redacted('CLOUDFLARE_QUEUES_TOKEN'),
})

// Pulling and acknowledging are both safe to retry: a lost pull response only
// leases messages until their visibility timeout, and Cloudflare accepts an
// acknowledgement for a lease more than once.
const RETRY_TIMES = 4
const RETRY_BASE = '500 millis'

export class QueueError extends Schema.TaggedError<QueueError>()('QueueError', { message: Schema.String }) {}

// Cloudflare's ID for a queue message, stable across redeliveries. Not an event ID:
// one event can arrive as several messages.
export const MessageId = Schema.String.pipe(Schema.brand('MessageId'))

export type MessageId = typeof MessageId.Type

// One delivery's handle for acknowledging or retrying its message.
export const LeaseId = Schema.String.pipe(Schema.brand('LeaseId'))

export type LeaseId = typeof LeaseId.Type

// Only the fields needed to acknowledge are required, so one odd message cannot
// fail the whole batch; its body is classified by the caller.
export const Message = Schema.Struct({
  id: MessageId,
  lease_id: LeaseId,
  body: Schema.optional(Schema.Unknown),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
})

export type Message = typeof Message.Type

const Pulled = Schema.Struct({ result: Schema.Struct({ messages: Schema.Array(Message) }) })

const Acked = Schema.Struct({ success: Schema.Boolean })

// Cloudflare's v4 API error envelope.
const ErrorBody = Schema.Struct({
  errors: Schema.Array(Schema.Struct({ code: Schema.optional(Schema.Number), message: Schema.String })),
})

export interface Retry {
  readonly leaseId: LeaseId
  readonly delaySeconds: number
}

export class QueueClient extends Context.Service<QueueClient, {
  // Leases up to batchSize messages until each is acknowledged, retried, or its
  // visibility timeout passes, after which Cloudflare redelivers it.
  readonly pull: (options: { readonly batchSize: number; readonly visibilityTimeoutMs: number }) =>
    Effect.Effect<ReadonlyArray<Message>, QueueError>
  // Acknowledged messages are deleted from the queue. Retried ones come back
  // after their delay, counting toward the consumer's max_retries.
  readonly ack: (acks: ReadonlyArray<LeaseId>, retries: ReadonlyArray<Retry>) => Effect.Effect<void, QueueError>
}>()('hermes-webhook-relay/QueueClient') {}

const failure = Effect.fnUntraced(function* (what: string, response: HttpClientResponse.HttpClientResponse) {
  const body = yield* HttpClientResponse.schemaBodyJson(ErrorBody)(response).pipe(Effect.option)
  const detail = Option.match(body, {
    onNone: () => '',
    onSome: ({ errors }) => errors.length === 0 ? '' : `: ${errors.map((error) => error.message).join('; ')}`,
  })
  return yield* new QueueError({ message: `${what}: ${response.status}${detail}` })
})

export const layer = (options: Config.Success<typeof config>): Layer.Layer<QueueClient, never, HttpClient.HttpClient> =>
  Layer.effect(QueueClient, Effect.gen(function* () {
    const base = `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(options.accountId)}` +
      `/queues/${encodeURIComponent(options.queueId)}/messages`
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest((request) => request.pipe(
        HttpClientRequest.prependUrl(base),
        HttpClientRequest.bearerToken(Redacted.value(options.token)),
      )),
      HttpClient.retryTransient({ times: RETRY_TIMES, schedule: Schedule.exponential(RETRY_BASE) }),
    )

    const post = <S extends Schema.Constraint>(path: string, body: object, schema: S) => {
      const what = `POST ${path}`
      return Effect.gen(function* () {
        const response = yield* client.execute(HttpClientRequest.post(path).pipe(HttpClientRequest.bodyJsonUnsafe(body)))
        if (response.status < 200 || response.status >= 300) return yield* failure(what, response)
        return yield* HttpClientResponse.schemaBodyJson(schema)(response)
      }).pipe(Effect.catchTags({
        HttpClientError: (e) => Effect.fail(new QueueError({ message: `${what}: ${e.message}` })),
        SchemaError: (e) => Effect.fail(new QueueError({ message: `${what}: unexpected answer: ${e.message}` })),
      }))
    }

    return QueueClient.of({
      pull: Effect.fn('QueueClient.pull')(function* ({ batchSize, visibilityTimeoutMs }) {
        const pulled = yield* post('/pull', { batch_size: batchSize, visibility_timeout_ms: visibilityTimeoutMs }, Pulled)
        return pulled.result.messages
      }),
      ack: Effect.fn('QueueClient.ack')(function* (acks, retries) {
        if (acks.length === 0 && retries.length === 0) return
        const acked = yield* post('/ack', {
          acks: acks.map((leaseId) => ({ lease_id: leaseId })),
          retries: retries.map((retry) => ({ lease_id: retry.leaseId, delay_seconds: retry.delaySeconds })),
        }, Acked)
        if (!acked.success) return yield* new QueueError({ message: 'POST /ack: not acknowledged' })
      }),
    })
  }))

export const layerConfig: Layer.Layer<QueueClient, Config.ConfigError, HttpClient.HttpClient> = Layer.unwrap(
  Effect.gen(function* () {
    return layer(yield* config)
  }),
)
