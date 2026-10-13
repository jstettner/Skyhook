import { Console, Effect, Schema } from 'effect'
import { Argument, Command, Flag } from 'effect/cli'
import * as Inbox from './Inbox/Inbox'
import * as Pull from './Pull/Pull'
import * as QueueClient from './Queue/QueueClient'
import * as SourceRecordProviders from './SourceRecord/SourceRecordProviders'

// Lines on stdout are the interface: Hermes compares `pending` output between
// runs, so it holds only what changes when the inbox does.

// Prints `value` as JSON, encoded with `schema` (refs as strings, times as ISO strings).
const printJson = <S extends Schema.Constraint>(schema: S) => (value: S['Type']) =>
  Schema.encodeEffect(schema)(value).pipe(
    Effect.orDie,
    Effect.flatMap((encoded) => Console.log(JSON.stringify(encoded, null, 2))),
  )

const PendingOutput = Schema.Struct({
  pending: Schema.Array(Schema.Struct({ ref: Inbox.RefFromString, ...Inbox.Entry.fields })),
  // Pending events beyond --limit.
  more: Schema.Number,
  quarantined: Schema.Number,
})

const ShowOutput = Schema.Struct({ ref: Inbox.RefFromString, ...Inbox.Entry.fields, record: Schema.Json })

const ref = Argument.String('event').pipe(
  Argument.withSchema(Inbox.RefFromString),
  Argument.withDescription('The event, as `provider:eventId` (from `pending`).'),
)

const pull = Command.make('pull', {}, () => Effect.gen(function* () {
  const summary = yield* Pull.run()
  yield* Console.log(
    `received ${summary.received}, duplicates ${summary.duplicates}, ` +
      `quarantined ${summary.quarantined}, held ${summary.held}`,
  )
})).pipe(
  Command.withDescription('Move new events from the Cloudflare queue into the inbox.'),
  Command.provide(QueueClient.layerConfig),
)

const pending = Command.make('pending', {
  limit: Flag.Int('limit').pipe(Flag.withDefault(20), Flag.withDescription('List at most this many events.')),
  json: Flag.Boolean('json').pipe(Flag.withDefault(false), Flag.withDescription('Print JSON.')),
}, ({ limit, json }) => Effect.gen(function* () {
  const inbox = yield* Inbox.Inbox
  const { entries, total } = yield* inbox.pending(limit)
  const quarantined = yield* inbox.quarantinedCount
  if (json) {
    return yield* printJson(PendingOutput)({
      pending: entries.map((entry) => ({ ref: entry.event, ...entry })),
      more: total - entries.length,
      quarantined,
    })
  }
  for (const { event } of entries) {
    yield* Console.log(`${Inbox.formatRef(event)} ${event.eventType} ${event.sourceRecordId} ${event.sourceTimestamp}`)
  }
  if (total > entries.length) yield* Console.log(`... and ${total - entries.length} more`)
  if (quarantined > 0) yield* Console.log(`quarantined ${quarantined} (see: relay-consumer quarantine)`)
})).pipe(Command.withDescription('List events not yet completed, oldest first.'))

const show = Command.make('show', {
  ref,
  transcript: Flag.Boolean('transcript').pipe(Flag.withDefault(false), Flag.withDescription('Include the transcript.')),
}, ({ ref, transcript }) => Effect.gen(function* () {
  const entry = yield* (yield* Inbox.Inbox).get(ref)
  const reader = yield* (yield* SourceRecordProviders.SourceRecordProviders).resolve(entry.event.provider)
  const record = yield* reader.read(entry.event.sourceRecordId, { transcript })
  yield* printJson(ShowOutput)({ ref, ...entry, record })
})).pipe(
  Command.withDescription("Print an event and the current state of its provider's record, as JSON."),
  Command.provide(SourceRecordProviders.layer),
)

const CompleteOutput = Schema.Struct({
  ref: Inbox.RefFromString,
  result: Schema.Literals(['completed', 'already_completed']),
})

const complete = Command.make('complete', {
  ref,
  json: Flag.Boolean('json').pipe(Flag.withDefault(false), Flag.withDescription('Print JSON.')),
}, ({ ref, json }) => Effect.gen(function* () {
  const result = (yield* (yield* Inbox.Inbox).complete(ref)) === 'Completed' ? 'completed' : 'already_completed'
  if (json) return yield* printJson(CompleteOutput)({ ref, result })
  yield* Console.log(`${result === 'completed' ? 'completed' : 'already completed'} ${Inbox.formatRef(ref)}`)
})).pipe(Command.withDescription('Mark an event handled, so `pending` no longer lists it.'))

const quarantine = Command.make('quarantine', {}, () => Effect.gen(function* () {
  yield* printJson(Schema.Array(Inbox.Quarantined))(yield* (yield* Inbox.Inbox).quarantined)
})).pipe(Command.withDescription('Print queue messages kept because they could not be stored as events, as JSON.'))

export const cli = Command.make('relay-consumer').pipe(
  Command.withDescription('Pulls hermes-webhook-relay events into a local inbox.'),
  Command.withSubcommands([pull, pending, show, complete, quarantine]),
)
