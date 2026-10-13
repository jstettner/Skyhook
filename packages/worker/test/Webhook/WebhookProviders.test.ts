import { describe, expect, it, layer } from '@effect/vitest'
import { ConfigProvider, Effect, Layer, Result } from 'effect'
import * as WebhookProviders from '../../src/Webhook/WebhookProviders'

const secrets = { GRANOLA_SIGNING_SECRET: 'whsec_dGVzdC1zZWNyZXQ=', POCKET_SIGNING_SECRET: 'pocket-secret' }
const fromEnv = (env: object) =>
  WebhookProviders.layer.pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env))))
// The tag a resolution ends with. A ConfigError from building the registry is caught
// where the layer is provided.
const tag = (name: string) => Effect.flatMap(WebhookProviders.WebhookProviders, (registry) => registry.resolve(name)).pipe(
  Effect.result,
  Effect.map((result) => Result.isFailure(result) ? result.failure._tag : 'Resolved'),
)

it.effect.each([
  ['granola,pocket', 'granola', 'Resolved'],
  [' granola , pocket ', 'pocket', 'Resolved'],
  ['granola', 'pocket', 'WebhookProviderNotFound'],
  [undefined, 'granola', 'WebhookProviderNotFound'],
  ['', 'granola', 'WebhookProviderNotFound'],
  [['granola', 'pocket'], 'pocket', 'Resolved'],
  [['granola'], 'pocket', 'WebhookProviderNotFound'],
  ['granola,pocket', 'other', 'WebhookProviderNotFound'],
  ['granola,pocket', 'toString', 'WebhookProviderNotFound'],
  // A malformed list fails layer construction, so every name fails alike.
  ['granola,other', 'granola', 'ConfigError'],
  ['granola,pocket,', 'granola', 'ConfigError'],
  // Only a real JSON array (a JSON var) is a list; a JSON string is not parsed.
  ['["granola"]', 'granola', 'ConfigError'],
  [['granola', 'other'], 'granola', 'ConfigError'],
] as const)('WEBHOOK_PROVIDERS=%j resolves %s as %s', ([enabled, name, expected]) =>
  tag(name).pipe(
    Effect.provide(fromEnv({ ...secrets, WEBHOOK_PROVIDERS: enabled })),
    Effect.catchTag('ConfigError', (error) => Effect.succeed(error._tag)),
    Effect.map((actual) => expect(actual).toBe(expected)),
  ))

describe.each([undefined, ''])('with POCKET_SIGNING_SECRET=%j', (value) => {
  layer(fromEnv({ ...secrets, WEBHOOK_PROVIDERS: 'granola,pocket', POCKET_SIGNING_SECRET: value }))((it) => {
    it.effect('treats the missing secret as misconfiguration for that provider', () =>
      Effect.map(tag('pocket'), (actual) => expect(actual).toBe('WebhookConfigurationError')))
    it.effect('still resolves the other provider', () =>
      Effect.map(tag('granola'), (actual) => expect(actual).toBe('Resolved')))
  })
})

layer(fromEnv({ ...secrets, WEBHOOK_PROVIDERS: 'granola', EVENTS: { send: () => {} } }))('with a queue binding', (it) => {
  it.effect('ignores non-string bindings', () =>
    Effect.map(tag('granola'), (actual) => expect(actual).toBe('Resolved')))
})

it.effect('fails layer construction, not resolution, for a malformed list', () => Effect.gen(function* () {
  const built = yield* Effect.result(Layer.build(fromEnv({ WEBHOOK_PROVIDERS: 'nope' })))
  expect(Result.isFailure(built) && built.failure._tag).toBe('ConfigError')
}))
