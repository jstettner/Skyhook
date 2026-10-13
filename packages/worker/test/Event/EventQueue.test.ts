import { describe, expect, it, vi } from '@effect/vitest'
import { Effect, Result } from 'effect'
import * as EventQueue from '../../src/Event/EventQueue'
import { event, sendResponse } from '../fixtures'

describe('live queue', () => {
  it.effect('sends only explicitly selected metadata as JSON', () => {
    const send = vi.fn(async () => sendResponse)
    return Effect.gen(function* () {
      yield* (yield* EventQueue.EventQueue).enqueue({ ...event, transcript: 'must not leave worker' } as typeof event)
      expect(send).toHaveBeenCalledExactlyOnceWith(event, { contentType: 'json' })
    }).pipe(Effect.provide(EventQueue.layer({ send })))
  })
  it.effect('sanitizes binding failures', () => Effect.gen(function* () {
    const result = yield* Effect.result((yield* EventQueue.EventQueue).enqueue(event))
    expect(Result.isFailure(result) && result.failure._tag).toBe('EnqueueFailed')
    expect(JSON.stringify(result)).not.toContain('secret')
  }).pipe(Effect.provide(EventQueue.layer({ send: async () => { throw new Error('secret') } }))))
})
