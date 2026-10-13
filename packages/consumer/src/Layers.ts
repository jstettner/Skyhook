import { SqliteClient } from '@effect/sql-sqlite-bun'
import { Effect, FileSystem, Layer, Path } from 'effect'
import { FetchHttpClient } from 'effect/http'
import * as Config from './Config'
import * as Inbox from './Inbox/Inbox'

// The inbox database at RELAY_INBOX, in a directory only this user can read.
const Sqlite = Layer.unwrap(Effect.gen(function* () {
  const filename = yield* Config.inboxPath
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  yield* fs.makeDirectory(path.dirname(filename), { recursive: true, mode: 0o700 })
  return SqliteClient.layer({ filename })
}))

// What every command uses. The queue client is provided to `pull` alone, so the
// other commands run without Cloudflare credentials.
export const Main = Layer.mergeAll(Inbox.layer.pipe(Layer.provide(Sqlite)), FetchHttpClient.layer)
