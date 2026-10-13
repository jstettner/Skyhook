import { Effect, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/http'
import * as SourceRecord from '../SourceRecord/SourceRecord'

const Answer = Schema.Struct({ data: Schema.Json })

// Reads recordings with a Pocket API key (`pk_…`).
// GET /public/recordings/{id}: https://docs.heypocketai.com/docs/api/recordings/get-recording-details
export const reader = (apiKey: Redacted.Redacted<string>): Effect.Effect<SourceRecord.Reader, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    return {
      read: Effect.fn('Pocket.read')(function* (recordingId, options) {
        const request = HttpClientRequest.get(
          `https://public.heypocketai.com/api/v1/public/recordings/${encodeURIComponent(recordingId)}`,
        ).pipe(
          HttpClientRequest.bearerToken(Redacted.value(apiKey)),
          HttpClientRequest.setUrlParam('include_transcript', String(options.transcript)),
        )
        const answer = yield* SourceRecord.getJson(client, 'Pocket GET /public/recordings', request, Answer)
        return answer.data
      }),
    }
  })
