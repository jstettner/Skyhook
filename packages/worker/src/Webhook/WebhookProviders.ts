import { Config, Context, Effect, Layer, Option, Schema, type Redacted } from 'effect'
import * as Granola from '../Provider/Granola'
import * as Pocket from '../Provider/Pocket'
import * as Webhook from './Webhook'

// Every provider the relay supports. Adding one means adding an entry here;
// deployments opt in through WEBHOOK_PROVIDERS plus the secret binding.
export const providers = {
  granola: { secretBinding: 'GRANOLA_SIGNING_SECRET', ingestion: Granola.ingestion },
  pocket: { secretBinding: 'POCKET_SIGNING_SECRET', ingestion: Pocket.ingestion },
} as const satisfies Record<string, {
  readonly secretBinding: string
  readonly ingestion: (secret: Redacted.Redacted<string>) => Effect.Effect<Webhook.Ingestion, Webhook.ConfigurationError>
}>

export type ProviderName = keyof typeof providers
export type ProviderSecrets = { readonly [P in ProviderName as typeof providers[P]['secretBinding']]?: string }

const names = Object.keys(providers) as [ProviderName, ...ProviderName[]]
// Unset or empty enables nothing; unknown names are a misconfiguration. Config.Array
// reads a JSON array or splits a string on commas without trimming, so trim each name.
const EnabledProviders = Config.Array(Schema.Trim.pipe(Schema.decodeTo(Schema.Literals(names))), 'WEBHOOK_PROVIDERS').pipe(
  Config.withDefault([] as ReadonlyArray<ProviderName>),
)

export class WebhookProviders extends Context.Service<WebhookProviders, {
  readonly resolve: (name: string) =>
    Effect.Effect<Webhook.Ingestion, Webhook.ProviderNotFound | Webhook.ConfigurationError>
}>()('hermes-webhook-relay/WebhookProviders') {}

// Reads all configuration when built, failing with a ConfigError if the enabled
// list is malformed. Each enabled provider's secret is optional here, so a
// missing secret fails only that provider's route rather than the whole layer.
export const layer = Layer.effect(WebhookProviders, Effect.gen(function* () {
  const enabled = yield* EnabledProviders
  const configured = new Map(yield* Effect.forEach(enabled, (name) =>
    Config.option(Config.Redacted(providers[name].secretBinding)).pipe(
      Effect.map((secret) => [name as string, { provider: providers[name], secret }] as const),
    )))
  return WebhookProviders.of({
    resolve: Effect.fn('WebhookProviders.resolve')(function* (name: string) {
      const entry = configured.get(name)
      if (entry === undefined) return yield* new Webhook.ProviderNotFound()
      if (Option.isNone(entry.secret)) return yield* new Webhook.ConfigurationError()
      return yield* entry.provider.ingestion(entry.secret.value)
    }),
  })
}))
