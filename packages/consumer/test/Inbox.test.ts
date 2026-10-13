import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { describe, expect, it, layer } from '@effect/vitest'
import { Effect, Option, Result, Runtime, Schema } from 'effect'
import * as Inbox from '../src/Inbox/Inbox'
import * as QueueClient from '../src/Queue/QueueClient'
import { layerInbox } from '../src/testing'

const event = (eventId: string) => Schema.decodeSync(EventEnvelope.EventEnvelope)({
  version: 1, provider: 'granola', eventId, eventType: 'note.generated',
  sourceRecordId: 'not_1d3tmYTlCICgjy', sourceTimestamp: '2026-09-16T12:00:00Z',
})

const ref = (eventId: string) => Schema.decodeSync(Inbox.Ref)({ provider: 'granola', eventId })

describe('RefFromString', () => {
  it.each([
    ['granola:event-1', Option.some({ provider: 'granola', eventId: 'event-1' })],
    // Only the first colon separates the provider.
    ['pocket:a:b', Option.some({ provider: 'pocket', eventId: 'a:b' })],
    ['granola:', Option.none()],
    ['event-1', Option.none()],
    ['otter:event-1', Option.none()],
  ])('decodes %s', (text, expected) => {
    expect(Schema.decodeUnknownOption(Inbox.RefFromString)(text)).toEqual(expected)
  })

  it('encodes back to the same text', () => {
    expect(Inbox.formatRef(Schema.decodeSync(Inbox.RefFromString)('pocket:a:b'))).toBe('pocket:a:b')
  })
})

layer(layerInbox)('Inbox', (it) => {
  it.effect('lists pending events in the order received, with how many there are in all', () => Effect.gen(function* () {
    const inbox = yield* Inbox.Inbox
    for (const id of ['order-b', 'order-a', 'order-c']) yield* inbox.receive(event(id))
    const { entries, total } = yield* inbox.pending(2)

    expect(entries.map((entry) => entry.event.eventId)).toEqual(['order-b', 'order-a'])
    expect(total).toBe(3)
    for (const id of ['order-b', 'order-a', 'order-c']) yield* inbox.complete(ref(id))
  }))

  it.effect('a completed event leaves pending, and completing it again says so', () => Effect.gen(function* () {
    const inbox = yield* Inbox.Inbox
    yield* inbox.receive(event('done-1'))

    expect(yield* inbox.complete(ref('done-1'))).toBe('Completed')
    expect(yield* inbox.complete(ref('done-1'))).toBe('AlreadyCompleted')
    expect((yield* inbox.pending(10)).entries.map((entry) => entry.event.eventId)).not.toContain('done-1')
    expect(Option.isSome((yield* inbox.get(ref('done-1'))).completedAt)).toBe(true)
  }))

  it.effect('a completed event is still recognized when it is delivered again', () => Effect.gen(function* () {
    const inbox = yield* Inbox.Inbox
    yield* inbox.receive(event('late-1'))
    yield* inbox.complete(ref('late-1'))

    expect(yield* inbox.receive(event('late-1'))).toBe('Duplicate')
    expect(Option.isSome((yield* inbox.get(ref('late-1'))).completedAt)).toBe(true)
  }))

  it('an event not in the inbox exits 3', () => {
    expect(Runtime.getErrorExitCode(new Inbox.EventNotFound({ ref: ref('missing') }))).toBe(3)
  })

  it.effect('an unknown event is not found', () => Effect.gen(function* () {
    const inbox = yield* Inbox.Inbox
    const tag = <A, E extends { readonly _tag: string }>(effect: Effect.Effect<A, E>) =>
      Effect.map(Effect.result(effect), (result) => Result.isFailure(result) && result.failure._tag)

    expect([yield* tag(inbox.get(ref('missing'))), yield* tag(inbox.complete(ref('missing')))])
      .toEqual(['EventNotFound', 'EventNotFound'])
  }))

  it.effect('a message ID is quarantined once', () => Effect.gen(function* () {
    const inbox = yield* Inbox.Inbox
    const messageId = Schema.decodeSync(QueueClient.MessageId)('message-q')
    yield* inbox.quarantine({ messageId, reason: 'invalid_body', body: 'first' })
    yield* inbox.quarantine({ messageId, reason: 'invalid_body', body: 'second' })

    expect((yield* inbox.quarantined).filter((kept) => kept.messageId === 'message-q').map((kept) => kept.body))
      .toEqual(['first'])
    expect(yield* inbox.quarantinedCount).toBe(1)
  }))
})
