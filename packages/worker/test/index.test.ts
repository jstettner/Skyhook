import { Effect } from 'effect'
import type { Bindings } from '../src/Layers'
import { expect, it, vi } from 'vitest'
import app from '../src/index'
import { event, fixtureApp, ingestFixture, providersFixture, sendResponse } from './fixtures'

const environment = (send = vi.fn(async () => sendResponse)) => ({
  WEBHOOK_PROVIDERS: 'granola,pocket',
  GRANOLA_SIGNING_SECRET: 'whsec_dGVzdC1zZWNyZXQ=',
  POCKET_SIGNING_SECRET: 'pocket-test-secret',
  EVENTS: { send, sendBatch: vi.fn(async () => sendResponse), metrics: vi.fn(async () => sendResponse.metadata.metrics) },
}) satisfies Bindings

it.each(['granola', 'pocket'].flatMap((provider) =>
  ['{}', 'not json', '{"transcript":"private"}'].map((body) => [provider, body])),
)('production fails closed: %s %s', async (provider, body) => {
  const env = environment()
  const response = await app.request(`/webhooks/${provider}`, { method: 'POST', body, headers: { 'signature': 'fake' } }, env)
  expect(response.status).toBe(401)
  expect(await response.json()).toEqual({ error: 'unauthorized' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it.each([
  ['nothing is enabled by default', 'granola', undefined],
  ['an empty list', 'granola', ''],
  ['a provider not in the list', 'pocket', 'granola'],
  ['an unregistered provider', 'other', 'granola,pocket'],
  ['an inherited property name', 'constructor', 'granola,pocket'],
])('returns 404 for %s', async (_case, provider, enabled) => {
  const env = { ...environment(), WEBHOOK_PROVIDERS: enabled }
  const response = await app.request(`/webhooks/${provider}`, { method: 'POST' }, env)
  expect(response.status).toBe(404)
  expect(await response.json()).toEqual({ error: 'not_found' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it.each<Bindings['WEBHOOK_PROVIDERS']>(['granola,pocket', ' granola , pocket ', ['granola', 'pocket']])('accepts the provider list %j', async (enabled) => {
  const env = { ...environment(), WEBHOOK_PROVIDERS: enabled }
  for (const provider of ['granola', 'pocket']) {
    expect((await app.request(`/webhooks/${provider}`, { method: 'POST' }, env)).status).toBe(401)
  }
})
it.each<Bindings['WEBHOOK_PROVIDERS']>(['granola,other', '["granola"]', 'GRANOLA', 'granola,pocket,', ['granola', 'other']])('returns sanitized 500 on every webhook path for a malformed provider list: %j', async (enabled) => {
  const env = { ...environment(), WEBHOOK_PROVIDERS: enabled }
  for (const provider of ['granola', 'other']) {
    const response = await app.request(`/webhooks/${provider}`, { method: 'POST' }, env)
    expect(response.status).toBe(500)
    expect(await response.json()).toEqual({ error: 'internal_error' })
  }
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it.each([
  ['granola', 'GRANOLA_SIGNING_SECRET'],
  ['pocket', 'POCKET_SIGNING_SECRET'],
])('returns sanitized 500 when %s is enabled without a secret, without affecting the other route', async (provider, binding) => {
  const env = { ...environment(), [binding]: undefined }
  const response = await app.request(`/webhooks/${provider}`, { method: 'POST' }, env)
  expect(response.status).toBe(500)
  expect(await response.json()).toEqual({ error: 'internal_error' })
  const other = provider === 'granola' ? 'pocket' : 'granola'
  expect((await app.request(`/webhooks/${other}`, { method: 'POST' }, env)).status).toBe(401)
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it('awaits successful storage before 202', async () => {
  let complete!: () => void
  let started!: () => void
  const entered = new Promise<void>((resolve) => { started = resolve })
  const stored = new Promise<void>((resolve) => { complete = resolve })
  const send = vi.fn(async () => { started(); await stored; return sendResponse })
  let settled = false
  const response = fixtureApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, environment(send))
  const pending = Promise.resolve(response).then((value) => { settled = true; return value })
  await entered
  expect(settled).toBe(false)
  complete()
  expect((await pending).status).toBe(202)
  expect(send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
})
it('fixture providers do not require live configuration', async () => {
  const env = { ...environment(), WEBHOOK_PROVIDERS: undefined, GRANOLA_SIGNING_SECRET: undefined }
  const response = await fixtureApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(202)
  expect(env.EVENTS.send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
})
it('routes each path to its own provider', async () => {
  const pocketEvent = { ...event, provider: 'pocket' as const }
  const env = environment()
  const routed = fixtureApp(providersFixture({
    granola: Effect.succeed({ ingest: () => Effect.die(new Error('wrong provider')) }),
    pocket: Effect.succeed({ ingest: () => Effect.succeed(pocketEvent) }),
  }))
  expect((await routed.request('/webhooks/pocket', { method: 'POST' }, env)).status).toBe(202)
  expect(env.EVENTS.send).toHaveBeenCalledExactlyOnceWith(pocketEvent, { contentType: 'json' })
})
it('returns sanitized 503 on storage failure', async () => {
  const env = environment(vi.fn(async () => { throw new Error('secret') }))
  const response = await fixtureApp(ingestFixture).request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(503)
  expect(await response.json()).toEqual({ error: 'enqueue_failed' })
})
it.each([
  () => Effect.die(new Error('secret')),
  () => { throw new Error('secret') },
])('sanitizes unexpected defects', async (ingest) => {
  const env = environment()
  const response = await fixtureApp(providersFixture({ granola: Effect.succeed({ ingest }) }))
    .request('/webhooks/granola', { method: 'POST' }, env)
  expect(response.status).toBe(500)
  expect(await response.json()).toEqual({ error: 'internal_error' })
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
it('preserves root and does not enqueue for other routes or methods', async () => {
  const env = environment()
  expect(await (await app.request('/', {}, env)).text()).toBe('Hello Hono!')
  expect((await app.request('/webhooks/granola', {}, env)).status).toBe(404)
  expect((await app.request('/other', { method: 'POST' }, env)).status).toBe(404)
  expect(env.EVENTS.send).not.toHaveBeenCalled()
})
