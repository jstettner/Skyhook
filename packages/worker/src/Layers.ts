import { ConfigProvider, Layer, type Config } from 'effect'
import * as EventQueue from './Event/EventQueue'
import * as WebhookProviders from './Webhook/WebhookProviders'

// Per-deployment values are set as Cloudflare secrets (or in .dev.vars locally),
// not checked into Wrangler configuration. They are read through Effect Config.
export type Bindings = CloudflareBindings & WebhookProviders.ProviderSecrets & {
  WEBHOOK_PROVIDERS?: string | ReadonlyArray<string>
}

// The single service boundary: everything a request needs, built from the bindings.
// Construction fails on a malformed WEBHOOK_PROVIDERS, which the edge maps to 500.
export type MakeAppLayer = (env: Bindings) =>
  Layer.Layer<WebhookProviders.WebhookProviders | EventQueue.EventQueue, Config.ConfigError>

// Config reads the bindings directly, never process.env. Empty strings count as unset.
export const AppLayer: MakeAppLayer = (env) =>
  Layer.mergeAll(WebhookProviders.layer, EventQueue.layer(env.EVENTS)).pipe(
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env))),
  )
