import type { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { Effect, Runtime, Schedule, Schema } from 'effect'
import { HttpClient, type HttpClientRequest, HttpClientResponse } from 'effect/http'

// Reading failed, or the provider answered something that isn't the expected JSON.
export class ReadFailed extends Schema.TaggedError<ReadFailed>()('SourceRecordReadFailed', { message: Schema.String }) {}

// HTTP statuses meaning the record is gone or this key may not read it, as opposed
// to a failed read: trying again won't help.
const UNAVAILABLE = new Set([403, 404, 410])

// The provider says the record doesn't exist or isn't readable with this key, such
// as a deleted recording. Exits 4, so a caller can tell it from a failed read.
export class Unavailable extends Schema.TaggedError<Unavailable>()('SourceRecordUnavailable', {
  what: Schema.String,
  status: Schema.Number,
}) {
  readonly [Runtime.errorExitCode] = 4

  override get message() {
    return `${this.what}: ${this.status}: the record doesn't exist or isn't readable with this API key`
  }
}

// The provider's API key isn't set. Never attach the key itself to errors.
export class NotConfigured extends Schema.TaggedError<NotConfigured>()('SourceRecordNotConfigured', {
  provider: Schema.String,
  apiKeyVariable: Schema.String,
}) {
  override get message() {
    return `set ${this.apiKeyVariable} to read ${this.provider} records`
  }
}

export interface Options {
  // Include the transcript, which can be far larger than the rest of the record.
  readonly transcript: boolean
}

// What `show` needs from a provider: the current state of the record an event
// names, as the provider's API returns it.
export interface Reader {
  readonly read: (sourceRecordId: EventEnvelope.SourceRecordId, options: Options) =>
    Effect.Effect<Schema.Json, ReadFailed | Unavailable>
}

const RETRY_TIMES = 3
const RETRY_BASE = '500 millis'
// Error bodies are quoted in messages; never more than this.
const MAX_ERROR_DETAIL = 300

// GETs `request` (a read, so safe to retry) and decodes the JSON answer.
export const getJson = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  client: HttpClient.HttpClient,
  what: string,
  request: HttpClientRequest.HttpClientRequest,
  schema: S,
) {
  const retrying = client.pipe(
    HttpClient.retryTransient({ times: RETRY_TIMES, schedule: Schedule.exponential(RETRY_BASE) }),
  )
  return yield* Effect.gen(function* () {
    const response = yield* retrying.execute(request)
    if (UNAVAILABLE.has(response.status)) return yield* new Unavailable({ what, status: response.status })
    if (response.status < 200 || response.status >= 300) {
      const text = yield* response.text.pipe(Effect.orElseSucceed(() => ''))
      const detail = text.slice(0, MAX_ERROR_DETAIL)
      return yield* new ReadFailed({ message: `${what}: ${response.status}${detail ? `: ${detail}` : ''}` })
    }
    return yield* HttpClientResponse.schemaBodyJson(schema)(response)
  }).pipe(Effect.catchTags({
    HttpClientError: (e) => Effect.fail(new ReadFailed({ message: `${what}: ${e.message}` })),
    SchemaError: (e) => Effect.fail(new ReadFailed({ message: `${what}: unexpected answer: ${e.message}` })),
  }))
})
