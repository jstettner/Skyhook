import type { EventEnvelope } from '@hermes-webhook-relay/envelope'
import { Config, Context, Effect, Layer, Option, type Redacted } from 'effect'
import { HttpClient } from 'effect/http'
import * as Granola from '../Provider/Granola'
import * as Pocket from '../Provider/Pocket'
import * as SourceRecord from './SourceRecord'

// How to read the records each provider's events name. Every provider the envelope
// allows needs an entry; a deployment configures the API key of those it uses.
export const providers = {
  granola: { apiKeyVariable: 'GRANOLA_API_KEY', reader: Granola.reader },
  pocket: { apiKeyVariable: 'POCKET_API_KEY', reader: Pocket.reader },
} as const satisfies {
  readonly [P in EventEnvelope.Provider]: {
    readonly apiKeyVariable: string
    readonly reader: (apiKey: Redacted.Redacted<string>) => Effect.Effect<SourceRecord.Reader, never, HttpClient.HttpClient>
  }
}

const names = Object.keys(providers) as Array<EventEnvelope.Provider>

export class SourceRecordProviders extends Context.Service<SourceRecordProviders, {
  readonly resolve: (provider: EventEnvelope.Provider) => Effect.Effect<SourceRecord.Reader, SourceRecord.NotConfigured>
}>()('hermes-webhook-relay/SourceRecordProviders') {}

// Reads every provider's API key when built. Each is optional here, so a missing
// key fails only that provider's records.
export const layer = Layer.effect(SourceRecordProviders, Effect.gen(function* () {
  const client = yield* HttpClient.HttpClient
  const keys = new Map(yield* Effect.forEach(names, (name) =>
    Config.option(Config.Redacted(providers[name].apiKeyVariable)).pipe(Effect.map((key) => [name, key] as const))))
  return SourceRecordProviders.of({
    resolve: Effect.fn('SourceRecordProviders.resolve')(function* (provider) {
      const { apiKeyVariable, reader } = providers[provider]
      const key = keys.get(provider) ?? Option.none()
      if (Option.isNone(key)) return yield* new SourceRecord.NotConfigured({ provider, apiKeyVariable })
      return yield* reader(key.value).pipe(Effect.provideService(HttpClient.HttpClient, client))
    }),
  })
}))
