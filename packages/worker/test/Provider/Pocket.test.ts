import { expect, it, vi } from '@effect/vitest'
import { Effect, Redacted, Result } from 'effect'
import { TestClock } from 'effect/testing'
import * as Pocket from '../../src/Provider/Pocket'
import { EventQueue } from '../../src/Event/EventQueue'
import * as WebhookHandler from '../../src/Webhook/WebhookHandler'
import app from '../../src/index'
import { providersFixture, sendResponse } from '../fixtures'

const pocketHandler = WebhookHandler.handle('pocket')

const secret = 'pocket-test-secret'
const now = 1800000000000
const payload = {
  event: 'summary.completed',
  timestamp: '2026-02-18T12:00:00.000Z',
  user: { id: 'user_abc123', email: 'alice@example.com' },
  recording: { id: 'rec_abc123', title: 'Team Standup', recordingAt: '2026-02-17T15:02:00.000Z' },
  summarizations: { sum_456: { v2: { summary: { markdown: '## Private notes' } } } },
  transcript: [{ speaker: 'SPEAKER_00', speakerName: 'Alice', text: 'Private words.', start: 0, end: 3.5 }],
  speakers: { SPEAKER_00: { name: 'Alice', speakerId: 'spk_abc123' } },
}
const eventId = async (event: string, recordingId: string, timestamp: string) =>
  [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`${event}\n${recordingId}\n${timestamp}`)))]
    .map((byte) => byte.toString(16).padStart(2, '0')).join('')
const expected = {
  version: 1, provider: 'pocket', eventId: await eventId(payload.event, payload.recording.id, payload.timestamp),
  eventType: payload.event, sourceRecordId: payload.recording.id, sourceTimestamp: payload.timestamp,
}
async function sign(body: string | Uint8Array<ArrayBuffer>, timestamp: string, key = secret) {
  const hmac = await crypto.subtle.importKey('raw', new TextEncoder().encode(key),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const prefix = new TextEncoder().encode(`${timestamp}.`)
  const bytes = typeof body === 'string' ? new TextEncoder().encode(body) : body
  const message = new Uint8Array(prefix.length + bytes.length)
  message.set(prefix)
  message.set(bytes, prefix.length)
  return [...new Uint8Array(await crypto.subtle.sign('HMAC', hmac, message))]
    .map((byte) => byte.toString(16).padStart(2, '0')).join('')
}
const signed = (body: string | Uint8Array<ArrayBuffer> = JSON.stringify(payload), timestamp = String(now), key = secret) =>
  Effect.promise(async () => new Request('https://example.com/webhooks/pocket', {
    method: 'POST', body,
    headers: {
      'content-type': 'application/json',
      'x-heypocket-timestamp': timestamp,
      'x-heypocket-signature': await sign(body, timestamp, key),
    },
  }))
const json = (response: Response) => Effect.promise(() => response.json())
// Handles the request at the fixed wall time `now`.
const handle = Effect.fn(function* (request: Request, configuredSecret = secret) {
  yield* TestClock.setTime(now)
  const enqueue = vi.fn(() => Effect.void)
  const response = yield* pocketHandler(request).pipe(
    Effect.provide(providersFixture({ pocket: Pocket.ingestion(Redacted.make(configuredSecret)) })),
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

it.effect('normalizes a signed delivery and forwards no meeting content', () => Effect.gen(function* () {
  const { response, enqueue } = yield* handle(yield* signed())
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  const forwarded = JSON.stringify(enqueue.mock.calls)
  for (const secretText of ['Private', 'alice@example.com', 'Team Standup']) expect(forwarded).not.toContain(secretText)
}))
it.effect.each([
  'transcription.completed', 'summary.completed', 'summary.regenerated', 'summary.updated',
  'mind_map.completed', 'action_items.regenerated', 'speakers.labeled', 'transcript.edited',
  'action_items.updated', 'recording.created', 'recording.deleted', 'recording.merged', 'translation.completed',
])('accepts documented event %s', (event) => Effect.gen(function* () {
  const { response, enqueue } = yield* handle(yield* signed(JSON.stringify({ ...payload, event })))
  expect(response.status).toBe(202)
  expect(enqueue).toHaveBeenCalledExactlyOnceWith({
    ...expected, eventType: event, eventId: yield* Effect.promise(() => eventId(event, payload.recording.id, payload.timestamp)),
  })
}))
it('runs live ingestion through the app', async () => {
  const send = vi.fn(async () => sendResponse)
  const response = await app.request(await Effect.runPromise(signed(undefined, String(Date.now()))), {}, {
    WEBHOOK_PROVIDERS: 'pocket', POCKET_SIGNING_SECRET: secret,
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(202)
  expect(send).toHaveBeenCalledExactlyOnceWith(expected, { contentType: 'json' })
})
it('reads a secret containing commas from the bindings unsplit', async () => {
  const send = vi.fn(async () => sendResponse)
  const response = await app.request(await Effect.runPromise(signed(undefined, String(Date.now()), 'a,b,c')), {}, {
    WEBHOOK_PROVIDERS: 'pocket', POCKET_SIGNING_SECRET: 'a,b,c',
    EVENTS: { send, sendBatch: vi.fn(), metrics: vi.fn() },
  })
  expect(response.status).toBe(202)
})
it.effect('keeps the event ID when a retry is re-signed with a new header timestamp', () => Effect.gen(function* () {
  const first = yield* handle(yield* signed(undefined, String(now)))
  const retry = yield* handle(yield* signed(undefined, String(now + 5000)))
  expect(first.enqueue).toHaveBeenCalledExactlyOnceWith(expected)
  expect(retry.enqueue).toHaveBeenCalledExactlyOnceWith(expected)
}))
it.effect('derives different event IDs for different events on the same recording', () => Effect.gen(function* () {
  const other = yield* handle(yield* signed(JSON.stringify({ ...payload, event: 'transcript.edited' })))
  expect(other.enqueue.mock.calls[0]).not.toContainEqual(expect.objectContaining({ eventId: expected.eventId }))
}))
it.effect.each([-300001, -300000, 300000, 300001])('enforces freshness at %s ms', (offset) => Effect.gen(function* () {
  const { response, enqueue } = yield* handle(yield* signed(undefined, String(now + offset)))
  expect(response.status).toBe(Math.abs(offset) > 300000 ? 401 : 202)
  expect(enqueue).toHaveBeenCalledTimes(Math.abs(offset) > 300000 ? 0 : 1)
}))
it.effect('rejects a seconds timestamp as stale', () => Effect.gen(function* () {
  yield* rejected(yield* signed(undefined, String(now / 1000)), 401)
}))
it.effect.each(['x-heypocket-timestamp', 'x-heypocket-signature'])('rejects missing %s (legacy unsigned webhooks)', (header) => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.delete(header)
  yield* rejected(request, 401)
}))
it.effect('rejects an unsigned delivery with a valid payload', () => Effect.gen(function* () {
  yield* rejected(new Request('https://example.com/webhooks/pocket', { method: 'POST', body: JSON.stringify(payload) }), 401)
}))
it.effect.each([
  ['x-heypocket-timestamp', '1800000000000junk'], ['x-heypocket-timestamp', '1.8e12'],
  ['x-heypocket-signature', 'zz'.repeat(32)], ['x-heypocket-signature', 'ab'.repeat(31)],
  ['x-heypocket-signature', 'ab'.repeat(33)], ['x-heypocket-signature', `v1,${'ab'.repeat(32)}`],
  ['x-heypocket-signature', 'x'.repeat(4097)],
])('rejects malformed %s: %s', ([header, value]) => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.set(header, value)
  yield* rejected(request, 401)
}))
it.effect('accepts an optional sha256= prefix and uppercase hex', () => Effect.gen(function* () {
  const request = yield* signed()
  request.headers.set('x-heypocket-signature', `sha256=${request.headers.get('x-heypocket-signature')!.toUpperCase()}`)
  expect((yield* handle(request)).response.status).toBe(202)
}))
it.effect('rejects a wrong key, tampered bytes, and a signature over the body alone', () => Effect.gen(function* () {
  yield* rejected(yield* signed(undefined, String(now), 'wrong-secret'), 401)
  const request = yield* signed()
  yield* rejected(new Request(request, { body: JSON.stringify(payload) + ' ' }), 401)
  const bodyOnly = yield* signed()
  const signature = yield* Effect.promise(async () => {
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
    return [...new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(JSON.stringify(payload))))]
      .map((byte) => byte.toString(16).padStart(2, '0')).join('')
  })
  bodyOnly.headers.set('x-heypocket-signature', signature)
  yield* rejected(bodyOnly, 401)
}))
it.effect('verifies before parsing JSON', () => Effect.gen(function* () {
  const request = yield* signed()
  yield* rejected(new Request(request, { body: 'not json' }), 401)
}))
it.effect.each([
  'not json', 'null', '[]', '{}',
  JSON.stringify({ ...payload, event: 'unknown.event' }),
  JSON.stringify({ ...payload, recording: undefined }),
  JSON.stringify({ ...payload, recording: { id: 'rec.bad' } }),
  JSON.stringify({ ...payload, timestamp: '2026-02-30T12:00:00.000Z' }),
  JSON.stringify({ ...payload, timestamp: '2026-02-18T12:00:00+00:00' }),
])('rejects authenticated invalid payload: %s', (body) => Effect.gen(function* () {
  const response = yield* rejected(yield* signed(body), 400)
  expect(yield* json(response)).toEqual({ error: 'invalid_payload' })
}))
it.effect('accepts a large transcript up to the 2 MiB limit and rejects one byte more', () => Effect.gen(function* () {
  const body = JSON.stringify(payload)
  expect((yield* handle(yield* signed(body.padEnd(2 * 1024 * 1024, ' ')))).response.status).toBe(202)
  yield* rejected(yield* signed(body.padEnd(2 * 1024 * 1024 + 1, ' ')), 413)
}))
it.effect('rejects an oversized secret', () => Effect.gen(function* () {
  const result = yield* Effect.result(Pocket.ingestion(Redacted.make('x'.repeat(1025))))
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure._tag).toBe('WebhookConfigurationError')
}))
