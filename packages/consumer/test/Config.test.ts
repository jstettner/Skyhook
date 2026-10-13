import { expect, it } from '@effect/vitest'
import { Config as EffectConfig, ConfigProvider, Effect, FileSystem, Layer } from 'effect'
import * as Config from '../src/Config'

// `files` is the file system: path to contents.
const withFiles = (env: object, files: Record<string, string>) => {
  const fs = FileSystem.layerNoop({
    exists: (path) => Effect.succeed(path in files),
    readFileString: (path) => Effect.succeed(files[path] ?? ''),
  })
  return Effect.provide(Config.layerEnvFile.pipe(
    Layer.provide(fs),
    Layer.provideMerge(ConfigProvider.layer(ConfigProvider.fromUnknown(env))),
  ))
}

const read = EffectConfig.all({ id: EffectConfig.String('CLOUDFLARE_QUEUE_ID'), token: EffectConfig.String('CLOUDFLARE_QUEUES_TOKEN') })

it.effect('settings come from the env file, with the environment winning', () => Effect.gen(function* () {
  const settings = yield* read.pipe(withFiles(
    { HOME: '/home/me', CLOUDFLARE_QUEUE_ID: 'from-env' },
    { '/home/me/.config/hermes-webhook-relay/env': 'CLOUDFLARE_QUEUE_ID=from-file\nCLOUDFLARE_QUEUES_TOKEN=secret\n' },
  ))

  expect(settings).toEqual({ id: 'from-env', token: 'secret' })
}))

it.effect('RELAY_ENV_FILE names another env file', () => Effect.gen(function* () {
  const settings = yield* read.pipe(withFiles(
    { HOME: '/home/me', RELAY_ENV_FILE: '/etc/relay.env' },
    { '/etc/relay.env': 'CLOUDFLARE_QUEUE_ID=q\nCLOUDFLARE_QUEUES_TOKEN=t\n' },
  ))

  expect(settings).toEqual({ id: 'q', token: 't' })
}))

it.effect('without an env file, the environment alone is read', () => Effect.gen(function* () {
  const settings = yield* read.pipe(withFiles({ HOME: '/home/me', CLOUDFLARE_QUEUE_ID: 'q', CLOUDFLARE_QUEUES_TOKEN: 't' }, {}))

  expect(settings).toEqual({ id: 'q', token: 't' })
}))

it.effect('the inbox defaults to the XDG state directory', () => Effect.gen(function* () {
  expect(yield* Config.inboxPath.pipe(withFiles({ HOME: '/home/me' }, {})))
    .toBe('/home/me/.local/state/hermes-webhook-relay/inbox.sqlite')
}))
