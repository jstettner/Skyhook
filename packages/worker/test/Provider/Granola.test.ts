import { expect, it, vi } from '@effect/vitest'
import { Effect, Fiber, Redacted, Result } from 'effect'
import { TestClock } from 'effect/testing'
import * as Granola from '../../src/Provider/Granola'
import { EventQueue } from '../../src/Event/EventQueue'
import * as WebhookHandler from '../../src/Webhook/WebhookHandler'
import app from '../../src/index'
import { providersFixture, sendResponse } from '../fixtures'

const granolaHandler = WebhookHandler.handle('granola')

const secret = 'whsec_dGVzdC1zZWNyZXQ='
const now = 1800000000
const payload = {
  event_id: 'event_123', event_type: 'note.generated',
  note_id: 'not_1d3tmYTlCICgjy', occurred_at: '2026-01-27T15:30:00Z',
}
const expected = {
  version: 1, provider: 'granola', eventId: payload.event_id,
  eventType: payload.event_type, sourceRecordId: payload.note_id, sourceTimestamp: payload.occurred_at,
}
const signed = (body: string | Uint8Array<ArrayBuffer> = JSON.stringify(payload), timestamp = String(now)) => Effect.promise(async () => {
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode('test-secret'),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const prefix = new TextEncoder().encode(`${payload.event_id}.${timestamp}.`)
  const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
  const message = new Uint8Array(prefix.length + bytes.length)
  message.set(prefix)
  message.set(bytes, prefix.length)
  const signature = await crypto.subtle.sign('HMAC', key, message)
  return new Request('https://example.com/webhooks/granola', {
    method: 'POST', body,
    headers: {
      'webhook-id': payload.event_id, 'webhook-timestamp': timestamp,
      'webhook-signature': `v1,${btoa(String.fromCharCode(...new Uint8Array(signature)))}`,
    },
  })
})
const json = (response: Response) => Effect.promise(() => response.json())
// Handles the request at the fixed wall time `now`.
const handle = Effect.fn(function* (request: Request, configuredSecret = secret) {
  yield* TestClock.setTime(now * 1000)
  const enqueue = vi.fn(() => Effect.void)
  const response = yield* granolaHandler(request).pipe(
    Effect.provide(providersFixture({ granola: Granola.ingestion(Redacted.make(configuredSecret)) })),
    Effect.provideService(EventQueue, { enqueue }),
  )
  return { response, enqueue }
})
const rejected = Effect.fn(function* (request: Request, status: number) {
  const { response, enqueue } = yield* handle(request)
  expect(response.status).toBe(status)
  expect(enqueue).not.toHaveBeenCalled()
  return response
})

it.effect.each(['note.generated', 'note.edited', 'note.access_granted'])('normalizes signed %s and strips unknown fields', (event_type) => Effect.gen(function* () {
  const body = JSON.stringify({ ...payload, event_type, data: { changed_fields: ['summary'] }, private: '秘密' }, null, 2)
  const { response, enqueue } = yield* handle(yield* signed(body))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith({ ...expected, eventType: event_type })
}))
it.each([false, true])('runs live ingestion through the app (queue fails: %s)', async (fails) => {
  const send = vi.fn(async () => {
    if (fails) throw new Error('private queue error')
    return sendResponse
  })
  const request = await Effect.runPromise(signed(undefined, String(Math.floor(Date.now() / 1000))))
  const response = await app.request(request, {}, {
    WEBHOOK_PROVIDERS: 'granola', GRANOLA_SIGNING_SECRET: secret,
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(fails ? 503 : 202)
  expect(send).toHaveBeenCalledExactlyOnceWith(expected, { contentType: 'json' })
  if (fails) expect(await response.json()).toEqual({ error: 'enqueue_failed' })
})
it.effect('preserves event IDs on duplicate deliveries', () => Effect.gen(function* () {
  for (let i = 0; i < 2; i++) {
    const { enqueue } = yield* handle(yield* signed())
    expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  }
}))
it.effect.each([-301, -300, 300, 301])('enforces freshness at %s seconds', (offset) => Effect.gen(function* () {
  const { response, enqueue } = yield* handle(yield* signed(undefined, String(now + offset)))
  expect(response.status).toBe(Math.abs(offset) > 300 ? 401 : 202)
  expect(enqueue).toHaveBeenCalledTimes(Math.abs(offset) > 300 ? 0 : 1)
}))
it.effect.each(['webhook-id', 'webhook-timestamp', 'webhook-signature'])('rejects missing %s', (header) => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.delete(header)
  yield* rejected(request, 401)
}))
it.effect.each([
  ['webhook-id', 'event.123'], ['webhook-id', 'x'.repeat(513)],
  ['webhook-timestamp', '1800000000junk'], ['webhook-timestamp', '1.8e9'],
  ['webhook-signature', 'v1,%%%'], ['webhook-signature', 'v1,YQ=='],
  ['webhook-signature', 'v2,unknown'], ['webhook-signature', 'x'.repeat(4097)],
  ['webhook-signature', Array(17).fill('v2,unknown').join(' ')],
])('rejects malformed %s: %s', ([header, value]) => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.set(header, value)
  yield* rejected(request, 401)
}))
it.effect('accepts any matching v1 signature alongside other versions and nonmatching signatures', () => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.set('webhook-signature', `v2,unknown v1,%%% v1,${btoa('\0'.repeat(32))} ${request.headers.get('webhook-signature')}`)
  expect((yield* handle(request)).response.status).toBe(202)
}))
it.effect('rejects a wrong key and tampered raw bytes', () => Effect.gen(function* () {
  const wrongKey = yield* handle(yield* signed(), 'whsec_d3Jvbmcta2V5')
  expect(wrongKey.response.status).toBe(401)
  expect(wrongKey.enqueue).not.toHaveBeenCalled()
  const request = yield* signed()
  yield* rejected(new Request(request, { body: JSON.stringify(payload) + ' ' }), 401)
}))
it.effect.each([
  'not json', 'null', '[]',
  JSON.stringify({ ...payload, event_id: 'different' }),
  JSON.stringify({ ...payload, event_type: 'unknown' }),
  JSON.stringify({ ...payload, event_type: 'note.edited' }),
  JSON.stringify({ ...payload, event_type: 'note.edited', data: { changed_fields: ['other'] } }),
  JSON.stringify({ ...payload, note_id: 'not_bad' }),
  JSON.stringify({ ...payload, occurred_at: '2026-02-30T15:30:00Z' }),
])('rejects authenticated invalid payload: %s', (body) => Effect.gen(function* () {
  const response = yield* rejected(yield* signed(body), 400)
  expect(yield* json(response)).toEqual({ error: 'invalid_payload' })
}))
it.effect('rejects authenticated invalid UTF-8 rather than replacing invalid bytes', () => Effect.gen(function* () {
  const bytes = new Uint8Array(new TextEncoder().encode(JSON.stringify({ ...payload, extra: 'x' })))
  bytes[bytes.length - 3] = 0xff
  const response = yield* rejected(yield* signed(bytes), 400)
  expect(yield* json(response)).toEqual({ error: 'invalid_payload' })
}))
it.effect('reads exact raw bytes across chunk boundaries and releases the reader', () => Effect.gen(function* () {
  const text = JSON.stringify({ ...payload, extra: '秘密' }, null, 2)
  const request = yield* signed(text)
  const bytes = new TextEncoder().encode(text)
  const stream = new ReadableStream({
    start(controller) {
      for (const byte of bytes) controller.enqueue(Uint8Array.of(byte))
      controller.close()
    },
  })
  const { response, enqueue } = yield* handle(new Request(request, { body: stream, duplex: 'half' } as RequestInit))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  expect(stream.locked).toBe(false)
}))
it.effect('verifies before parsing JSON', () => Effect.gen(function* () {
  const request = yield* signed()
  yield* rejected(new Request(request, { body: 'not json' }), 401)
}))
it.effect.each([false, true])('limits streamed bytes and cancels overflow (cancel rejects: %s)', (cancelRejects) => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.set('content-length', '1')
  const cancel = vi.fn(async () => {
    if (cancelRejects) throw new Error('private cancellation failure')
  })
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new Uint8Array(32768))
      controller.enqueue(new Uint8Array(32769))
    }, cancel,
  })
  yield* rejected(new Request(request, { body: stream, duplex: 'half' } as RequestInit), 413)
  expect(cancel).toHaveBeenCalledOnce()
  expect(stream.locked).toBe(false)
}))
it.effect('cancels a stalled body on ingestion timeout', () => Effect.gen(function* () {
  yield* TestClock.setTime(now * 1000)
  const cancel = vi.fn()
  const stream = new ReadableStream({ cancel })
  const request = new Request(yield* signed(), { body: stream, duplex: 'half' } as RequestInit)
  const enqueue = vi.fn(() => Effect.void)
  // Import the key up front: the timeout starts at ingest, so the fiber must reach
  // the stalled read without pending real async work before the clock moves.
  const ingestion = yield* Granola.ingestion(Redacted.make(secret))
  const fiber = yield* granolaHandler(request).pipe(
    Effect.provide(providersFixture({ granola: Effect.succeed(ingestion) })),
    Effect.provideService(EventQueue, { enqueue }),
    Effect.forkChild,
  )
  yield* TestClock.adjust('10 seconds')
  const response = yield* Fiber.join(fiber)
  expect(response.status).toBe(500)
  expect(cancel).toHaveBeenCalledOnce()
  expect(stream.locked).toBe(false)
  expect(enqueue).not.toHaveBeenCalled()
}))
it.effect('maps a failed body stream to a sanitized 400', () => Effect.gen(function* () {
  const request = yield* signed()
  const stream = new ReadableStream({ start(controller) { controller.error(new Error('private')) } })
  yield* rejected(new Request(request, { body: stream, duplex: 'half' } as RequestInit), 400)
}))
it.effect('accepts the exact body-size limit', () => Effect.gen(function* () {
  const body = JSON.stringify(payload)
  expect((yield* handle(yield* signed(body.padEnd(65536, ' ')))).response.status).toBe(202)
}))
it.effect.each(['test-secret', 'whsec_', 'whsec_%%%', 'whsec_YR==', 'whsec_YQ'])('rejects an invalid secret format without exposing it: %s', (value) => Effect.gen(function* () {
  const result = yield* Effect.result(Granola.ingestion(Redacted.make(value)))
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) {
    expect(result.failure._tag).toBe('WebhookConfigurationError')
    expect(JSON.stringify(result.failure)).not.toContain('whsec_')
  }
}))
