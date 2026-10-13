# hermes-webhook-relay

Cloudflare Worker (Hono + Effect v4) that relays provider webhooks. A Bun workspace:
`packages/envelope` (the shared event schema, platform-free) and `packages/worker` (the Worker).
Dependency versions live in the root `package.json` catalog.

## Commands

```
bun run typecheck  # tsc -b over every package
bun run test
bun run dev        # wrangler dev
```

## Effect v4 reference

This project uses **Effect v4**. Much Effect code online (and in model training data) is v3 and
uses APIs that no longer exist. Before writing or changing Effect code, check idioms against the
local Effect source checkout at `~/.local/share/effect-solutions/effect`.
