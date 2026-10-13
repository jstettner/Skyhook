import { BunRuntime, BunServices } from '@effect/platform-bun'
import { Cause, Console, Effect, Option } from 'effect'
import { CliError, Command } from 'effect/cli'
import * as Cli from './Cli'
import * as Config from './Config'
import * as Layers from './Layers'

// An expected failure (a queue or provider error, missing configuration, an unknown
// event) is one line on stderr; anything else keeps its full cause. The CLI has
// already rendered its own usage errors. Either way the process exits 1.
const report = (cause: Cause.Cause<unknown>) => Option.match(Cause.findErrorOption(cause), {
  onNone: () => Cause.hasInterruptsOnly(cause) ? Effect.void : Console.error(Cause.pretty(cause)),
  onSome: (error) => CliError.isCliError(error)
    ? Effect.void
    : Console.error(`relay-consumer: ${error instanceof Error ? error.message : String(error)}`),
})

Command.run(Cli.cli, { version: '0.1.0' }).pipe(
  Effect.provide(Layers.Main),
  Effect.provide(Config.layerEnvFile),
  Effect.tapCause(report),
  Effect.provide(BunServices.layer),
  BunRuntime.runMain({ disableErrorReporting: true }),
)
