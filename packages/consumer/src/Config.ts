import { Config, ConfigProvider, Effect, FileSystem, Layer } from 'effect'

const home = Config.String('HOME')

// Hermes runs cron scripts with credentials stripped from the environment, so
// settings can also live in a dotenv file. The environment wins over the file.
export const envFile = Config.String('RELAY_ENV_FILE').pipe(
  Config.orElse(() => home.pipe(Config.map((dir) => `${dir}/.config/hermes-webhook-relay/env`))),
)

export const inboxPath = Config.String('RELAY_INBOX').pipe(
  Config.orElse(() => home.pipe(Config.map((dir) => `${dir}/.local/state/hermes-webhook-relay/inbox.sqlite`))),
)

// Adds the env file, when there is one, behind the environment.
export const layerEnvFile = Layer.unwrap(Effect.gen(function* () {
  const path = yield* envFile
  const fs = yield* FileSystem.FileSystem
  if (!(yield* fs.exists(path))) return Layer.empty
  return ConfigProvider.layerAdd(ConfigProvider.fromDotEnv({ path }))
}))
