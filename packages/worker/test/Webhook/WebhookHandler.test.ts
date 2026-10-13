import { expect, it, vi } from '@effect/vitest'
import { Effect, Redacted } from 'effect'
import { EnqueueFailed, EventQueue } from '../../src/Event/EventQueue'
import * as Granola from '../../src/Provider/Granola'
import * as WebhookHandler from '../../src/Webhook/WebhookHandler'
import { event, ingestFixture, providersFixture, request } from '../fixtures'

const granolaHandler = WebhookHandler.handle('granola')

it.effect('enqueues the ingested envelope and returns 202', () => {
  const enqueue = vi.fn(() => Effect.void)
  return Effect.gen(function* () {
    const response = yield* granolaHandler(request())
    expect(response.status).toBe(202)
    expect(enqueue).toHaveBeenCalledExactlyOnceWith(event)
  }).pipe(Effect.provide(ingestFixture), Effect.provideService(EventQueue, { enqueue }))
})
it.effect('fails closed without calling the queue', () => {
  const enqueue = vi.fn(() => Effect.void)
  return Effect.gen(function* () {
    const response = yield* granolaHandler(request())
    expect(response.status).toBe(401)
    expect(yield* Effect.promise(() => response.json())).toEqual({ error: 'unauthorized' })
    expect(enqueue).not.toHaveBeenCalled()
  }).pipe(
    Effect.provide(providersFixture({ granola: Granola.ingestion(Redacted.make('whsec_dGVzdC1zZWNyZXQ=')) })),
    Effect.provideService(EventQueue, { enqueue }),
  )
})
it.effect('maps enqueue failure to a sanitized 503', () => Effect.gen(function* () {
  const response = yield* granolaHandler(request())
  expect(response.status).toBe(503)
  expect(yield* Effect.promise(() => response.json())).toEqual({ error: 'enqueue_failed' })
}).pipe(
  Effect.provide(ingestFixture),
  Effect.provideService(EventQueue, { enqueue: () => Effect.fail(new EnqueueFailed()) }),
))
