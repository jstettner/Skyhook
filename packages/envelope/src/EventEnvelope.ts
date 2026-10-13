import { DateTime, Exit, Schema } from 'effect'

const Identifier = Schema.String.pipe(Schema.check(Schema.isMinLength(1), Schema.isMaxLength(512)))
const decodeTimestamp = Schema.decodeUnknownExit(Schema.DateTimeUtcFromString)

export const SourceTimestamp = Schema.String.pipe(Schema.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/),
  Schema.makeFilter((value: string) => {
    const decoded = decodeTimestamp(value)
    // Reject calendar rollover while preserving the provider's original string.
    return Exit.isSuccess(decoded) && DateTime.formatIso(decoded.value) ===
      (value.length === 20 ? value.replace('Z', '.000Z') : value)
  }, { message: 'Expected a valid UTC ISO timestamp' }),
)).annotate({ identifier: 'SourceTimestamp' })

export const EventEnvelope = Schema.Struct({
  version: Schema.Literal(1),
  provider: Schema.Literals(['granola', 'pocket']),
  eventId: Identifier,
  eventType: Identifier,
  sourceRecordId: Identifier,
  sourceTimestamp: SourceTimestamp,
}).annotate({ identifier: 'EventEnvelope' })

export type EventEnvelope = typeof EventEnvelope.Type

export const decode = Schema.decodeUnknownEffect(EventEnvelope, {
  onExcessProperty: 'error',
})
