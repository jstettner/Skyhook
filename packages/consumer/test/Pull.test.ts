import { describe, expect, it } from '@effect/vitest'
import { Effect, Fiber, Layer, Result } from 'effect'
import * as Base64 from 'effect/encoding/Base64'
import { TestClock } from 'effect/testing'
import * as Inbox from '../src/Inbox/Inbox'
import * as Pull from '../src/Pull/Pull'
import { layerInbox, layerQueue, makeQueueApi, type QueueApi } from '../src/testing'

const event = {
  version: 1, provider: 'granola', eventId: 'event-1', eventType: 'note.generated',
  sourceRecordId: 'not_1d3tmYTlCICgjy', sourceTimestamp: '2026-09-16T12:00:00Z',
} as const

// How Cloudflare documents a `json` message body over HTTP pull.
const base64 = (value: unknown) => Base64.encode(new TextEncoder().encode(JSON.stringify(value)))

// Runs one pull against `api`, then reads back the inbox it filled.
const pullInto = (api: QueueApi) => Effect.gen(function* () {
  const summary = yield* Pull.run()
  const inbox = yield* Inbox.Inbox
  return { summary, pending: yield* inbox.pending(100), quarantined: yield* inbox.quarantined }
})

const withQueue = (api: QueueApi) => Effect.provide(Layer.merge(layerQueue(api), layerInbox))

describe('Pull', () => {
  it.effect.each([
    ['base64 JSON', base64(event)],
    ['JSON text', JSON.stringify(event)],
  ])('stores a new event from a %s body, then acknowledges it', ([, body]) => Effect.gen(function* () {
    const api = makeQueueApi()
    api.publish({ body })
    const { summary, pending } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary).toEqual({ received: 1, duplicates: 0, quarantined: 0, held: 0 })
    expect(pending.entries.map((entry) => entry.event)).toEqual([event])
    expect(api.remaining()).toEqual([])
  }))

  it.effect('a redelivered event is stored once and both copies are acknowledged', () => Effect.gen(function* () {
    const api = makeQueueApi()
    api.publish({ body: base64(event) })
    api.publish({ body: base64(event) })
    const { summary, pending } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary).toEqual({ received: 1, duplicates: 1, quarantined: 0, held: 0 })
    expect(pending.total).toBe(1)
    expect(api.acked).toEqual(['message-1', 'message-2'])
  }))

  it.effect('a different event under a stored event ID is quarantined and the stored one kept', () =>
    Effect.gen(function* () {
      const api = makeQueueApi()
      api.publish({ body: base64(event) })
      api.publish({ body: base64({ ...event, sourceRecordId: 'not_00000000000000' }) })
      const { summary, pending, quarantined } = yield* pullInto(api).pipe(withQueue(api))

      expect(summary.quarantined).toBe(1)
      expect(pending.entries.map((entry) => entry.event)).toEqual([event])
      expect(quarantined.map((message) => [message.messageId, message.reason])).toEqual([['message-2', 'conflicting_event']])
      expect(api.remaining()).toEqual([])
    }))

  it.effect.each([
    ['not JSON', { body: base64('not json').slice(0, -2) + '!!' }, 'invalid_body'],
    ['truncated base64', { body: 'eyJ' }, 'invalid_body'],
    ['JSON that is not an object', { body: base64([event]) }, 'invalid_envelope'],
    ['broken JSON text', { body: '{"version":' }, 'invalid_json'],
    ['a missing field', { body: base64({ ...event, eventId: undefined }) }, 'invalid_envelope'],
    ['an extra field', { body: base64({ ...event, transcript: 'private' }) }, 'invalid_envelope'],
    ['a text message', { body: JSON.stringify(event), contentType: 'text' }, 'unsupported_content_type'],
    ['a non-string body', { body: 42 }, 'invalid_body'],
  ] as const)('a message with %s is quarantined and acknowledged', ([, message, reason]) => Effect.gen(function* () {
    const api = makeQueueApi()
    api.publish(message)
    const { summary, pending, quarantined } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary).toEqual({ received: 0, duplicates: 0, quarantined: 1, held: 0 })
    expect(pending.total).toBe(0)
    expect(quarantined.map((kept) => kept.reason)).toEqual([reason])
    expect(api.remaining()).toEqual([])
  }))

  it.effect('a quarantined body is kept for inspection', () => Effect.gen(function* () {
    const api = makeQueueApi()
    const body = base64({ ...event, eventId: '' })
    api.publish({ body })
    const { quarantined } = yield* pullInto(api).pipe(withQueue(api))

    expect(quarantined[0]?.body).toBe(body)
  }))

  it.effect.each([
    ['a provider this consumer does not know', { ...event, provider: 'otter' }],
    ['a newer envelope version', { ...event, version: 2 }],
  ])('an event from %s is retried later, not stored or acknowledged', ([, body]) => Effect.gen(function* () {
    const api = makeQueueApi()
    api.publish({ body: base64(body) })
    const { summary, pending, quarantined } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary).toEqual({ received: 0, duplicates: 0, quarantined: 0, held: 1 })
    expect(pending.total).toBe(0)
    expect(quarantined).toEqual([])
    expect(api.acked).toEqual([])
    expect(api.retried).toEqual([{ id: 'message-1', delaySeconds: Pull.UNKNOWN_RETRY_DELAY_SECONDS }])
  }))

  it.effect('an event is stored before the acknowledgement, so a failed one only causes a duplicate', () => {
    // More failures than the client retries.
    const api = makeQueueApi({ ackFailures: 5 })
    api.publish({ body: base64(event) })

    return Effect.gen(function* () {
      const failed = yield* Pull.run().pipe(Effect.result, Effect.forkChild)
      yield* TestClock.adjust('1 minute')
      const first = yield* Fiber.join(failed)
      expect(Result.isFailure(first) && first.failure._tag).toBe('QueueError')
      expect(api.remaining()).toEqual(['message-1'])

      api.expireLeases()
      const { summary, pending } = yield* pullInto(api)
      expect(summary).toEqual({ received: 0, duplicates: 1, quarantined: 0, held: 0 })
      expect(pending.total).toBe(1)
      expect(api.remaining()).toEqual([])
    }).pipe(withQueue(api))
  })

  it.effect('drains the queue in batches', () => Effect.gen(function* () {
    const api = makeQueueApi()
    for (let i = 0; i < 120; i++) api.publish({ body: base64({ ...event, eventId: `event-${i}` }) })
    const { summary } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary.received).toBe(120)
    expect(api.requests.filter((request) => request.endsWith('/pull'))).toHaveLength(3)
    expect(api.remaining()).toEqual([])
  }))

  it.effect('an empty queue is one pull and no acknowledgement', () => Effect.gen(function* () {
    const api = makeQueueApi()
    const { summary } = yield* pullInto(api).pipe(withQueue(api))

    expect(summary).toEqual({ received: 0, duplicates: 0, quarantined: 0, held: 0 })
    expect(api.requests).toEqual(['POST /client/v4/accounts/account/queues/queue/messages/pull'])
  }))
})
