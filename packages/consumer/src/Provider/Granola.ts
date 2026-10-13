import { Effect, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/http'
import * as SourceRecord from '../SourceRecord/SourceRecord'

// Reads notes with a Granola API key (Settings → API).
// GET /v1/notes/{id}: https://docs.granola.ai/api-reference/get-note
export const reader = (apiKey: Redacted.Redacted<string>): Effect.Effect<SourceRecord.Reader, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    return {
      read: Effect.fn('Granola.read')(function* (noteId, options) {
        const request = HttpClientRequest.get(`https://public-api.granola.ai/v1/notes/${encodeURIComponent(noteId)}`).pipe(
          HttpClientRequest.bearerToken(Redacted.value(apiKey)),
          options.transcript ? HttpClientRequest.setUrlParam('include', 'transcript') : (request) => request,
        )
        return yield* SourceRecord.getJson(client, 'Granola GET /v1/notes', request, Schema.Json, (status) =>
          status === 413
            ? `the transcript is too large to return inline; read it from /v1/notes/${noteId}/transcript`
            : undefined)
      }),
    }
  })
