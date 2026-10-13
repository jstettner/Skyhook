import { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { describe, expect, it } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Result, Schema } from 'effect'
import { HttpClient, type HttpClientRequest, HttpClientResponse } from 'effect/http'
import * as SourceRecordProviders from '../src/SourceRecord/SourceRecordProviders'

const keys = { GRANOLA_API_KEY: 'grn_test', POCKET_API_KEY: 'pk_test' }

const recordId = Schema.decodeSync(EventEnvelope.SourceRecordId)

// A provider API answering `status` with `body`, recording each request it served.
const fakeApi = (status: number, body: unknown) => {
  const requests: Array<{ readonly url: string; readonly authorization: string | undefined }> = []
  const client = HttpClient.make((request: HttpClientRequest.HttpClientRequest, url) => Effect.sync(() => {
    requests.push({ url: url.toString(), authorization: request.headers.authorization })
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

// Reads `id` through the provider's resolved reader.
const read = (provider: EventEnvelope.Provider, id: string, transcript = false) =>
  SourceRecordProviders.SourceRecordProviders.use((providers) => providers.resolve(provider)).pipe(
    Effect.flatMap((reader) => reader.read(recordId(id), { transcript })),
  )

describe('Granola', () => {
  it.effect('reads the note with the API key, adding the transcript only when asked', () => Effect.gen(function* () {
    const api = fakeApi(200, { id: 'not_1d3tmYTlCICgjy', title: 'Standup' })
    const record = yield* read('granola', 'not_1d3tmYTlCICgjy').pipe(api.provide())
    yield* read('granola', 'not_1d3tmYTlCICgjy', true).pipe(api.provide())

    expect(record).toEqual({ id: 'not_1d3tmYTlCICgjy', title: 'Standup' })
    expect(api.requests).toEqual([
      { url: 'https://public-api.granola.ai/v1/notes/not_1d3tmYTlCICgjy', authorization: 'Bearer grn_test' },
      { url: 'https://public-api.granola.ai/v1/notes/not_1d3tmYTlCICgjy?include=transcript', authorization: 'Bearer grn_test' },
    ])
  }))

  it.effect('a transcript too large to return inline points at the transcript endpoint', () => Effect.gen(function* () {
    const result = yield* read('granola', 'not_1d3tmYTlCICgjy', true).pipe(
      fakeApi(413, { message: 'TRANSCRIPT_TOO_LARGE' }).provide(),
      Effect.result,
    )

    expect(Result.isFailure(result) && result.failure.message).toBe(
      'Granola GET /v1/notes: 413: the transcript is too large to return inline; ' +
        'read it from /v1/notes/not_1d3tmYTlCICgjy/transcript',
    )
  }))
})

describe('Pocket', () => {
  it.effect('reads the recording and returns its data, without the transcript unless asked', () => Effect.gen(function* () {
    const api = fakeApi(200, { success: true, data: { id: 'rec-1', title: 'Walk' } })
    const record = yield* read('pocket', 'rec-1').pipe(api.provide())

    expect(record).toEqual({ id: 'rec-1', title: 'Walk' })
    expect(api.requests).toEqual([{
      url: 'https://public.heypocketai.com/api/v1/public/recordings/rec-1?include_transcript=false',
      authorization: 'Bearer pk_test',
    }])
  }))

  it.effect('a missing recording is an error naming the status', () => Effect.gen(function* () {
    const result = yield* read('pocket', 'gone').pipe(
      fakeApi(404, { success: false, error: 'Recording not found' }).provide(),
      Effect.result,
    )

    expect(Result.isFailure(result) && result.failure.message).toBe(
      'Pocket GET /public/recordings: 404: {"success":false,"error":"Recording not found"}',
    )
  }))
})

it.effect('a provider without its API key is not configured, makes no request, and the others still work', () =>
  Effect.gen(function* () {
    const api = fakeApi(200, { id: 'not_1d3tmYTlCICgjy' })
    const provide = api.provide({ GRANOLA_API_KEY: 'grn_test' })
    const pocket = yield* read('pocket', 'rec-1').pipe(provide, Effect.result)
    yield* read('granola', 'not_1d3tmYTlCICgjy').pipe(provide)

    expect(Result.isFailure(pocket) && pocket.failure.message).toBe('set POCKET_API_KEY to read pocket records')
    expect(api.requests.map((request) => request.url)).toEqual(['https://public-api.granola.ai/v1/notes/not_1d3tmYTlCICgjy'])
  }))
