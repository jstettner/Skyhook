import { Context, Effect, Layer, Schema } from 'effect'
import type { EventEnvelope } from '@hermes-webhook-relay/envelope'

export class EnqueueFailed extends Schema.TaggedError<EnqueueFailed>()('EnqueueFailed', {}) {}

export class EventQueue extends Context.Service<EventQueue, {
  readonly enqueue: (event: EventEnvelope.EventEnvelope) => Effect.Effect<void, EnqueueFailed>
}>()('hermes-webhook-relay/EventQueue') {}

// A function of the binding rather than Config: the queue is an object, not a value.
export const layer = (binding: Pick<Queue<EventEnvelope.EventEnvelope>, 'send'>) =>
  Layer.succeed(EventQueue, EventQueue.of({
    enqueue: Effect.fn('EventQueue.enqueue')(function* (event: EventEnvelope.EventEnvelope) {
      yield* Effect.tryPromise({
        try: async () => { await binding.send({
          version: event.version,
          provider: event.provider,
          eventId: event.eventId,
          eventType: event.eventType,
          sourceRecordId: event.sourceRecordId,
          sourceTimestamp: event.sourceTimestamp,
        }, { contentType: 'json' }) },
        catch: () => new EnqueueFailed(),
      })
    }),
  }))
