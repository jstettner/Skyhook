import { describe, expect, it } from '@effect/vitest'
import { Effect, Result } from 'effect'
import * as EventEnvelope from '../src/EventEnvelope'

const event = {
  version: 1, provider: 'granola', eventId: 'event-1', eventType: 'fixture',
  sourceRecordId: 'meeting-1', sourceTimestamp: '2026-09-16T12:00:00Z',
}

describe('envelope', () => {
  it.effect.each([
    event, { ...event, provider: 'pocket' },
    { ...event, sourceTimestamp: '2026-09-16T12:00:00.123Z' },
    { ...event, sourceTimestamp: '2024-02-29T12:00:00Z' },
  ])('accepts %j', (valid) => Effect.gen(function* () {
    expect(yield* EventEnvelope.decode(valid)).toEqual(valid)
  }))
  it.effect.each([
    { version: 2 }, { provider: 'other' }, { eventId: '' }, { eventType: 'x'.repeat(513) },
    { sourceRecordId: undefined }, { transcript: 'private' },
    { sourceTimestamp: '2026-02-30T12:00:00Z' }, { sourceTimestamp: 'not-a-date' },
    { sourceTimestamp: '2026-09-16T12:00:00+00:00' },
    { sourceTimestamp: '2026-02-29T12:00:00Z' },
    { sourceTimestamp: '2026-09-16T24:00:00Z' },
    { sourceTimestamp: '2026-09-16T12:00:00.12Z' },
    { sourceTimestamp: '2026-09-16T12:00:00.1234Z' },
  ])('rejects invalid metadata %j', (patch) => Effect.gen(function* () {
    const result = yield* Effect.result(EventEnvelope.decode({ ...event, ...patch }))
    expect(Result.isFailure(result)).toBe(true)
  }))
})
