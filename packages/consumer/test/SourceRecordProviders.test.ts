import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { describe, expect, it } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Result, Schema } from 'effect'
import { HttpClient, type HttpClientRequest, HttpClientResponse } from 'effect/http'
import * as SourceRecordProviders from '../src/SourceRecord/SourceRecordProviders'

const keys = { GRANOLA_API_KEY: 'grn_test', POCKET_API_KEY: 'pk_test' }

const recordId = Schema.decodeSync(EventEnvelope.SourceRecordId)

// A provider API: `answer` gives the status and body for each request URL. Records each request it served.
const fakeApi = (answer: (url: URL) => readonly [status: number, body: unknown]) => {
  const requests: Array<{ readonly url: string; readonly authorization: string | undefined }> = []
  const client = HttpClient.make((request: HttpClientRequest.HttpClientRequest, url) => Effect.sync(() => {
    requests.push({ url: url.toString(), authorization: request.headers.authorization })
    const [status, body] = answer(url)
    return HttpClientResponse.fromWeb(request, Response.json(body, { status }))
  }))
  return {
    requests,
    provide: (env: object = keys) => Effect.provide(SourceRecordProviders.layer.pipe(Layer.provide(Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient)(client),
      ConfigProvider.layer(ConfigProvider.fromUnknown(env)),
    )))),
  }
}

// Answers every request alike.
const always = (status: number, body: unknown) => fakeApi(() => [status, body])

// Reads `id` through the provider's resolved reader.
const read = (provider: EventEnvelope.Provider, id: string, transcript = false) =>
  SourceRecordProviders.SourceRecordProviders.use((providers) => providers.resolve(provider)).pipe(
    Effect.flatMap((reader) => reader.read(recordId(id), { transcript })),
  )

const NOTE = 'https://public-api.granola.ai/v1/notes/not_1d3tmYTlCICgjy'

const segment = (text: string) => ({ speaker: { source: 'microphone' }, text, start_time: '2026-09-16T12:00:00Z', end_time: '2026-09-16T12:00:05Z' })

// A Granola note whose transcript is `pages`, served a page per cursor.
const granola = (pages: ReadonlyArray<{ readonly segments: ReadonlyArray<string>; readonly cursor?: string | null }>) =>
  fakeApi((url) => {
    if (!url.pathname.endsWith('/transcript')) return [200, { id: 'not_1d3tmYTlCICgjy', title: 'Standup' }]
    const index = Number(url.searchParams.get('cursor') ?? 0)
    const page = pages[index] ?? { segments: [] }
    const next = index + 1 < pages.length
    return [200, {
      transcript: page.segments.map(segment),
      hasMore: next,
      cursor: page.cursor !== undefined ? page.cursor : next ? String(index + 1) : null,
    }]
  })

describe('Granola', () => {
  it.effect('reads the note with the API key, and nothing else without --transcript', () => Effect.gen(function* () {
    const api = granola([{ segments: ['hello'] }])
    const record = yield* read('granola', 'not_1d3tmYTlCICgjy').pipe(api.provide())

    expect(record).toEqual({ id: 'not_1d3tmYTlCICgjy', title: 'Standup' })
    expect(api.requests).toEqual([{ url: NOTE, authorization: 'Bearer grn_test' }])
  }))

  it.effect('with --transcript, follows every transcript page into the record', () => Effect.gen(function* () {
    const api = granola([{ segments: ['one', 'two'] }, { segments: ['three'] }])
    const record = yield* read('granola', 'not_1d3tmYTlCICgjy', true).pipe(api.provide())

    expect(record).toEqual({
      id: 'not_1d3tmYTlCICgjy',
      title: 'Standup',
      transcript: [segment('one'), segment('two'), segment('three')],
    })
    expect(api.requests.map((request) => request.url)).toEqual([
      NOTE,
      `${NOTE}/transcript?page_size=100`,
      `${NOTE}/transcript?page_size=100&cursor=1`,
    ])
  }))

  it.effect('a page that says there is more but gives no cursor is an error', () => Effect.gen(function* () {
    const api = granola([{ segments: ['one'], cursor: null }, { segments: ['two'] }])
    const result = yield* read('granola', 'not_1d3tmYTlCICgjy', true).pipe(api.provide(), Effect.result)

    expect(Result.isFailure(result) && result.failure.message).toBe('Granola transcript: more pages, but no cursor')
  }))
})

describe('Pocket', () => {
  it.effect('reads the recording and returns its data, without the transcript unless asked', () => Effect.gen(function* () {
    const api = always(200, { success: true, data: { id: 'rec-1', title: 'Walk' } })
    const record = yield* read('pocket', 'rec-1').pipe(api.provide())

    expect(record).toEqual({ id: 'rec-1', title: 'Walk' })
    expect(api.requests).toEqual([{
      url: 'https://public.heypocketai.com/api/v1/public/recordings/rec-1?include_transcript=false',
      authorization: 'Bearer pk_test',
    }])
  }))

  it.effect('a transcript the API returns anyway is dropped unless asked for', () => Effect.gen(function* () {
    const api = always(200, { success: true, data: { id: 'rec-1', transcript: { segments: [] } } })

    expect(yield* read('pocket', 'rec-1').pipe(api.provide())).toEqual({ id: 'rec-1' })
    expect(yield* read('pocket', 'rec-1', true).pipe(api.provide())).toEqual({ id: 'rec-1', transcript: { segments: [] } })
  }))

  it.effect('a missing recording is an error naming the status', () => Effect.gen(function* () {
    const result = yield* read('pocket', 'gone').pipe(
      always(404, { success: false, error: 'Recording not found' }).provide(),
      Effect.result,
    )

    expect(Result.isFailure(result) && result.failure.message).toBe(
      'Pocket GET /public/recordings: 404: {"success":false,"error":"Recording not found"}',
    )
  }))
})

it.effect('a provider without its API key is not configured, makes no request, and the others still work', () =>
  Effect.gen(function* () {
    const api = always(200, { id: 'not_1d3tmYTlCICgjy' })
    const provide = api.provide({ GRANOLA_API_KEY: 'grn_test' })
    const pocket = yield* read('pocket', 'rec-1').pipe(provide, Effect.result)
    yield* read('granola', 'not_1d3tmYTlCICgjy').pipe(provide)

    expect(Result.isFailure(pocket) && pocket.failure.message).toBe('set POCKET_API_KEY to read pocket records')
    expect(api.requests.map((request) => request.url)).toEqual(['https://public-api.granola.ai/v1/notes/not_1d3tmYTlCICgjy'])
  }))
