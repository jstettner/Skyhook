# hermes-webhook-relay

Cloudflare Worker (Hono + Effect v4) that relays provider webhooks, and `relay-consumer`, the CLI
that pulls them into a local inbox. A Bun workspace: `packages/envelope` (the shared event schema,
platform-free), `packages/worker` (the Worker), and `packages/consumer` (the CLI, built with Bun).
Dependency versions live in the root `package.json` catalog. The `overrides` entry holds
`@effect/platform-node-shared` (pulled in by `@effect/platform-bun`) at the catalog's Effect version, so
two Effect versions never meet at runtime; move it with the catalog.

## Commands

```
bun run typecheck  # tsc -b over every package
bun run test
bun run dev        # wrangler dev
bun run build      # packages/consumer/dist/relay-consumer
```

## Effect v4 reference

This project uses **Effect v4**. Much Effect code online (and in model training data) is v3 and
uses APIs that no longer exist. Before writing or changing Effect code, check idioms against the
local Effect source checkout at `~/.local/share/effect-solutions/effect`.
