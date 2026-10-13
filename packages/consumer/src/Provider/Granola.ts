import { Effect, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/http'
import * as SourceRecord from '../SourceRecord/SourceRecord'

const BASE = 'https://public-api.granola.ai/v1/notes'

const Note = Schema.Record(Schema.String, Schema.Json)

const TranscriptPage = Schema.Struct({
  transcript: Schema.Array(Schema.Json),
  hasMore: Schema.Boolean,
  cursor: Schema.NullOr(Schema.String),
})

// The endpoint's largest page.
const PAGE_SIZE = 100
// 50,000 segments, far beyond any meeting; only a cursor that never ends reaches it.
const MAX_PAGES = 500

// Reads notes with a Granola API key (Settings → API).
// GET /v1/notes/{id}: https://docs.granola.ai/api-reference/get-note
// GET /v1/notes/{id}/transcript: https://docs.granola.ai/api-reference/get-transcript
export const reader = (apiKey: Redacted.Redacted<string>): Effect.Effect<SourceRecord.Reader, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient
    const get = (url: string) => HttpClientRequest.get(url).pipe(HttpClientRequest.bearerToken(Redacted.value(apiKey)))

    // Get Note can include the transcript inline, but refuses one that is too large
    // (413), so the transcript is always read from its paginated endpoint.
    const transcript = Effect.fn('Granola.transcript')(function* (noteId: string) {
      const segments: Array<Schema.Json> = []
      let cursor: string | null = null
      for (let page = 0; page < MAX_PAGES; page++) {
        const request: HttpClientRequest.HttpClientRequest = get(`${BASE}/${encodeURIComponent(noteId)}/transcript`).pipe(
          HttpClientRequest.setUrlParam('page_size', String(PAGE_SIZE)),
          cursor === null ? (request) => request : HttpClientRequest.setUrlParam('cursor', cursor),
        )
        const answer: typeof TranscriptPage.Type = yield* SourceRecord.getJson(client, 'Granola GET /v1/notes/{id}/transcript', request, TranscriptPage)
        segments.push(...answer.transcript)
        if (!answer.hasMore) return segments
        if (answer.cursor === null) {
          return yield* new SourceRecord.ReadFailed({ message: 'Granola transcript: more pages, but no cursor' })
        }
        cursor = answer.cursor
      }
      return yield* new SourceRecord.ReadFailed({ message: `Granola transcript: more than ${MAX_PAGES} pages` })
    })

    return {
      read: Effect.fn('Granola.read')(function* (noteId, options) {
        const note = yield* SourceRecord.getJson(client, 'Granola GET /v1/notes', get(`${BASE}/${encodeURIComponent(noteId)}`), Note)
        return options.transcript ? { ...note, transcript: yield* transcript(noteId) } : note
      }),
    }
  })
