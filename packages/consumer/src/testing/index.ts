// In-memory stand-ins for the Cloudflare Queues HTTP pull API and the inbox
// database, so the consumer is tested without Cloudflare or a file.
import { SqliteClient } from '@effect/sql-sqlite-node'
import { Effect, Layer, Redacted, Schema } from 'effect'
import { HttpBody, HttpClient, type HttpClientRequest, HttpClientResponse } from 'effect/http'
import * as Inbox from '../Inbox/Inbox'
import * as QueueClient from '../Queue/QueueClient'

export interface Published {
  readonly body: unknown
  readonly contentType?: string
}

/** How the fake queue misbehaves. */
export interface QueueScript {
  /** `/ack` answers 500 this many times before it works. */
  readonly ackFailures?: number
}

/** A queue holding messages until they are acknowledged, behind an `HttpClient`. */
export interface QueueApi {
  readonly client: HttpClient.HttpClient
  readonly publish: (message: Published) => void
  /** Every request served, as `METHOD /path`, in order. */
  readonly requests: ReadonlyArray<string>
  /** Messages neither acknowledged nor retried, by ID. */
  readonly remaining: () => ReadonlyArray<string>
  readonly acked: ReadonlyArray<string>
  readonly retried: ReadonlyArray<{ readonly id: string; readonly delaySeconds: number | undefined }>
  /** Lets every lease lapse, as when the visibility timeout passes. */
  readonly expireLeases: () => void
}

const PullBody = Schema.fromJsonString(Schema.Struct({ batch_size: Schema.Number }))

const AckBody = Schema.fromJsonString(Schema.Struct({
  acks: Schema.Array(Schema.Struct({ lease_id: Schema.String })),
  retries: Schema.Array(Schema.Struct({ lease_id: Schema.String, delay_seconds: Schema.optional(Schema.Number) })),
}))

const bodyText = (request: HttpClientRequest.HttpClientRequest) =>
  request.body instanceof HttpBody.Uint8Array ? new TextDecoder().decode(request.body.body) : ''

const json = (request: HttpClientRequest.HttpClientRequest, status: number, body: unknown) =>
  HttpClientResponse.fromWeb(request, Response.json(body, { status }))

export const makeQueueApi = (script: QueueScript = {}): QueueApi => {
  const requests: Array<string> = []
  const acked: Array<string> = []
  const retried: Array<{ id: string; delaySeconds: number | undefined }> = []
  const messages = new Map<string, { readonly published: Published; lease: string | undefined }>()
  let published = 0
  let leases = 0
  let ackFailures = script.ackFailures ?? 0

  const byLease = (lease: string) => [...messages].find(([, message]) => message.lease === lease)?.[0]

  const pull = (request: HttpClientRequest.HttpClientRequest) => {
    const { batch_size } = Schema.decodeSync(PullBody)(bodyText(request))
    const batch = [...messages].filter(([, message]) => message.lease === undefined).slice(0, batch_size)
    return json(request, 200, {
      success: true,
      errors: [],
      result: {
        message_backlog_count: messages.size,
        messages: batch.map(([id, message]) => {
          message.lease = `lease-${++leases}`
          return {
            id,
            lease_id: message.lease,
            body: message.published.body,
            attempts: 1,
            timestamp_ms: 0,
            metadata: { 'CF-Content-Type': message.published.contentType ?? 'json' },
          }
        }),
      },
    })
  }

  const ack = (request: HttpClientRequest.HttpClientRequest) => {
    if (ackFailures > 0) {
      ackFailures--
      return json(request, 500, { success: false, errors: [{ code: 10000, message: 'internal error' }] })
    }
    const body = Schema.decodeSync(AckBody)(bodyText(request))
    for (const { lease_id } of body.acks) {
      const id = byLease(lease_id)
      if (id !== undefined) {
        messages.delete(id)
        acked.push(id)
      }
    }
    for (const { lease_id, delay_seconds } of body.retries) {
      const id = byLease(lease_id)
      if (id !== undefined) {
        messages.delete(id)
        retried.push({ id, delaySeconds: delay_seconds })
      }
    }
    return json(request, 200, {
      success: true,
      errors: [],
      result: { ackCount: body.acks.length, retryCount: body.retries.length, warnings: [] },
    })
  }

  const serve = (request: HttpClientRequest.HttpClientRequest, url: URL) => {
    requests.push(`${request.method} ${url.pathname}`)
    if (url.pathname.endsWith('/messages/pull')) return pull(request)
    if (url.pathname.endsWith('/messages/ack')) return ack(request)
    return json(request, 404, { success: false, errors: [{ code: 7003, message: 'not found' }] })
  }

  return {
    client: HttpClient.make((request, url) => Effect.sync(() => serve(request, url))),
    publish: (message) => {
      messages.set(`message-${++published}`, { published: message, lease: undefined })
    },
    requests,
    remaining: () => [...messages.keys()],
    acked,
    retried,
    expireLeases: () => {
      for (const message of messages.values()) message.lease = undefined
    },
  }
}

/** The queue client, talking to `api`. */
export const layerQueue = (api: QueueApi): Layer.Layer<QueueClient.QueueClient> =>
  QueueClient.layer({ accountId: 'account', queueId: 'queue', token: Redacted.make('queues-token') }).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient)(api.client)),
  )

/** An inbox in a fresh in-memory database. */
export const layerInbox: Layer.Layer<Inbox.Inbox> = Inbox.layer.pipe(
  Layer.provide(SqliteClient.layer({ filename: ':memory:' })),
  Layer.orDie,
)
