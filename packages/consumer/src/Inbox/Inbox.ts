import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { Context, DateTime, Effect, Layer, Option, Schema, SchemaGetter, SchemaTransformation } from 'effect'
import { SqlClient, type SqlError } from 'effect/sql'
import * as QueueClient from '../Queue/QueueClient'

// Event IDs are unique per provider, so this names one event.
export const Ref = Schema.Struct({ provider: EventEnvelope.Provider, eventId: EventEnvelope.EventId })

export type Ref = typeof Ref.Type

// A Ref as the command line writes it: `provider:eventId`. Provider names never
// contain a colon, so the first one separates the two.
// The pattern only gives a malformed ref a readable error; the parser does the work.
const refPattern = new RegExp(`^(${EventEnvelope.Provider.literals.join('|')}):.`)

export const RefFromString = Schema.String.pipe(
  Schema.check(Schema.isPattern(refPattern, { expected: 'provider:eventId, such as granola:evt_123' })),
  Schema.decodeTo(
    Schema.TemplateLiteralParser([EventEnvelope.Provider, ':', Schema.String]),
    SchemaTransformation.passthroughSupertype(),
  ),
  Schema.decodeTo(Ref, {
    decode: SchemaGetter.transform(([provider, , eventId]) => ({ provider, eventId })),
    encode: SchemaGetter.transform(({ provider, eventId }) => [provider, ':', eventId] as const),
  }),
).annotate({ identifier: 'Ref', description: 'an event as provider:eventId' })

export const formatRef: (ref: Ref) => string = Schema.encodeSync(RefFromString)

export class EventNotFound extends Schema.TaggedError<EventNotFound>()('EventNotFound', { ref: RefFromString }) {
  override get message() {
    return `no event ${formatRef(this.ref)} in the inbox`
  }
}

export const Entry = Schema.Struct({
  event: EventEnvelope.EventEnvelope,
  receivedAt: Schema.DateTimeUtcFromString,
  completedAt: Schema.OptionFromNullOr(Schema.DateTimeUtcFromString),
})

export type Entry = typeof Entry.Type

// Why a queue message was kept instead of stored as an event.
export const QuarantineReason = Schema.Literals([
  'unsupported_content_type', // not sent as JSON
  'invalid_body', // not text or base64
  'invalid_json',
  'invalid_envelope', // JSON, but not a valid envelope
  'conflicting_event', // a valid envelope reusing a stored event's ID
])

export type QuarantineReason = typeof QuarantineReason.Type

export const Quarantined = Schema.Struct({
  messageId: QueueClient.MessageId,
  reason: QuarantineReason,
  body: Schema.String,
  receivedAt: Schema.DateTimeUtcFromString,
})

export type Quarantined = typeof Quarantined.Type

export type Received = 'Inserted' | 'Duplicate' | 'Conflict'

export class Inbox extends Context.Service<Inbox, {
  // Stores an event as pending. A redelivery of a stored event is a Duplicate; a
  // different event under a stored provider and event ID is a Conflict, and the
  // stored one is kept. Either way the queue message can be acknowledged.
  readonly receive: (event: EventEnvelope.EventEnvelope) => Effect.Effect<Received, SqlError.SqlError>
  // Keeps a message that will never be an event, so it can be acknowledged
  // without being lost. A message ID already kept is ignored.
  readonly quarantine: (message: Omit<Quarantined, 'receivedAt'>) => Effect.Effect<void, SqlError.SqlError>
  // The oldest `limit` pending events, in the order they were received, and how many are pending in all.
  readonly pending: (limit: number) =>
    Effect.Effect<{ readonly entries: ReadonlyArray<Entry>; readonly total: number }, SqlError.SqlError>
  readonly get: (ref: Ref) => Effect.Effect<Entry, EventNotFound | SqlError.SqlError>
  readonly complete: (ref: Ref) =>
    Effect.Effect<'Completed' | 'AlreadyCompleted', EventNotFound | SqlError.SqlError>
  readonly quarantined: Effect.Effect<ReadonlyArray<Quarantined>, SqlError.SqlError>
  readonly quarantinedCount: Effect.Effect<number, SqlError.SqlError>
}>()('hermes-webhook-relay/Inbox') {}

// Columns decode with the same schemas as the values written into them.
const EventRow = Schema.Struct({
  provider: EventEnvelope.Provider,
  event_id: EventEnvelope.EventId,
  event_type: EventEnvelope.EventType,
  source_record_id: EventEnvelope.SourceRecordId,
  source_timestamp: EventEnvelope.SourceTimestamp,
  received_at: Schema.DateTimeUtcFromString,
  completed_at: Schema.OptionFromNullOr(Schema.DateTimeUtcFromString),
})

const QuarantineRow = Schema.Struct({
  message_id: QueueClient.MessageId,
  reason: QuarantineReason,
  body: Schema.String,
  received_at: Schema.DateTimeUtcFromString,
})

const Count = Schema.Struct({ count: Schema.Number })

// Rows are written only by this module, so a row that doesn't decode is a defect.
const decodeRows = <S extends Schema.Constraint>(schema: S) => (rows: ReadonlyArray<unknown>) =>
  Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(Effect.orDie)

const toEntry = (row: typeof EventRow.Type): Entry => ({
  event: {
    version: 1,
    provider: row.provider,
    eventId: row.event_id,
    eventType: row.event_type,
    sourceRecordId: row.source_record_id,
    sourceTimestamp: row.source_timestamp,
  },
  receivedAt: row.received_at,
  completedAt: row.completed_at,
})

const now = Effect.map(DateTime.now, DateTime.formatIso)

// Completed events are kept: they are what recognizes a late redelivery as a duplicate.
export const layer: Layer.Layer<Inbox, SqlError.SqlError, SqlClient.SqlClient> = Layer.effect(Inbox, Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  // A message is acknowledged once receive returns, so the commit must be on disk by then.
  yield* sql`PRAGMA synchronous = FULL`
  yield* sql`CREATE TABLE IF NOT EXISTS events (
    provider TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source_timestamp TEXT NOT NULL,
    received_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (provider, event_id)
  )`
  yield* sql`CREATE TABLE IF NOT EXISTS quarantine (
    message_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    body TEXT NOT NULL,
    received_at TEXT NOT NULL
  )`

  const count = (query: Effect.Effect<ReadonlyArray<unknown>, SqlError.SqlError>) =>
    query.pipe(Effect.flatMap(decodeRows(Count)), Effect.map((rows) => rows[0]?.count ?? 0))

  const find = (ref: Ref) =>
    sql`SELECT * FROM events WHERE provider = ${ref.provider} AND event_id = ${ref.eventId}`.pipe(
      Effect.flatMap(decodeRows(EventRow)),
      Effect.map((rows) => Option.fromNullishOr(rows[0])),
    )

  const findOrFail = (ref: Ref) => find(ref).pipe(
    Effect.flatMap(Option.match({
      onNone: () => Effect.fail(new EventNotFound({ ref })),
      onSome: Effect.succeed,
    })),
  )

  return Inbox.of({
    receive: Effect.fn('Inbox.receive')(function* (event) {
      return yield* sql.withTransaction(Effect.gen(function* () {
        const stored = yield* find(event)
        if (Option.isSome(stored)) {
          const row = stored.value
          return row.event_type === event.eventType && row.source_record_id === event.sourceRecordId &&
            row.source_timestamp === event.sourceTimestamp ? 'Duplicate' : 'Conflict'
        }
        yield* sql`INSERT INTO events ${sql.insert({
          provider: event.provider,
          event_id: event.eventId,
          event_type: event.eventType,
          source_record_id: event.sourceRecordId,
          source_timestamp: event.sourceTimestamp,
          received_at: yield* now,
        })}`
        return 'Inserted' as const
      }))
    }),
    quarantine: Effect.fn('Inbox.quarantine')(function* ({ messageId, reason, body }) {
      yield* sql`INSERT OR IGNORE INTO quarantine ${sql.insert({
        message_id: messageId,
        reason,
        body,
        received_at: yield* now,
      })}`
    }),
    pending: Effect.fn('Inbox.pending')(function* (limit) {
      const entries = yield* sql`SELECT * FROM events WHERE completed_at IS NULL ORDER BY rowid LIMIT ${limit}`.pipe(
        Effect.flatMap(decodeRows(EventRow)),
      )
      const total = yield* count(sql`SELECT count(*) AS count FROM events WHERE completed_at IS NULL`)
      return { entries: entries.map(toEntry), total }
    }),
    get: Effect.fn('Inbox.get')(function* (ref) {
      return toEntry(yield* findOrFail(ref))
    }),
    complete: Effect.fn('Inbox.complete')(function* (ref) {
      const row = yield* findOrFail(ref)
      if (Option.isSome(row.completed_at)) return 'AlreadyCompleted' as const
      yield* sql`UPDATE events SET completed_at = ${yield* now}
        WHERE provider = ${ref.provider} AND event_id = ${ref.eventId} AND completed_at IS NULL`
      return 'Completed' as const
    }),
    quarantined: sql`SELECT * FROM quarantine ORDER BY rowid`.pipe(
      Effect.flatMap(decodeRows(QuarantineRow)),
      Effect.map((rows) => rows.map((row) => ({
        messageId: row.message_id,
        reason: row.reason,
        body: row.body,
        receivedAt: row.received_at,
      }))),
    ),
    quarantinedCount: count(sql`SELECT count(*) AS count FROM quarantine`),
  })
}))
