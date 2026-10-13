import { DateTime, Exit, Schema } from 'effect'

const Identifier = Schema.String.pipe(Schema.check(Schema.isMinLength(1), Schema.isMaxLength(512)))

// Each identifier is branded, so an event ID can't be passed where a record ID (or a
// queue message ID) is expected. Values get a brand only by being decoded.

// Unique per provider: a redelivered event keeps its ID.
export const EventId = Identifier.pipe(Schema.brand('EventId'))

export type EventId = typeof EventId.Type

export const EventType = Identifier.pipe(Schema.brand('EventType'))

export type EventType = typeof EventType.Type

// The provider's record the event is about: a Granola note, a Pocket recording.
export const SourceRecordId = Identifier.pipe(Schema.brand('SourceRecordId'))

export type SourceRecordId = typeof SourceRecordId.Type
const decodeTimestamp = Schema.decodeUnknownExit(Schema.DateTimeUtcFromString)

export const SourceTimestamp = Schema.String.pipe(Schema.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/),
  Schema.makeFilter((value: string) => {
    const decoded = decodeTimestamp(value)
    // Reject calendar rollover while preserving the provider's original string.
    return Exit.isSuccess(decoded) && DateTime.formatIso(decoded.value) ===
      (value.length === 20 ? value.replace('Z', '.000Z') : value)
  }, { message: 'Expected a valid UTC ISO timestamp' }),
), Schema.brand('SourceTimestamp')).annotate({ identifier: 'SourceTimestamp' })

export type SourceTimestamp = typeof SourceTimestamp.Type

// Every provider the relay can enqueue. Consumers built from an older version may
// see a provider missing here, and should hold such events rather than drop them.
export const Provider = Schema.Literals(['granola', 'pocket']).annotate({ identifier: 'Provider' })

export type Provider = typeof Provider.Type

export const EventEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  provider: Provider,
  eventId: EventId,
  eventType: EventType,
  sourceRecordId: SourceRecordId,
  sourceTimestamp: SourceTimestamp,
}).annotate({ identifier: 'EventEnvelope' })

export type EventEnvelope = typeof EventEnvelope.Type

export const decode = Schema.decodeUnknownEffect(EventEnvelope, {
  onExcessProperty: 'error',
})
