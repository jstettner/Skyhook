# hermes-webhook-relay

A Hono Cloudflare Worker with Effect v4 webhook ingestion and queueing for
Granola and Pocket.

## Configuration

Providers are opt-in. Each deployment sets `WEBHOOK_PROVIDERS` (a comma-separated
list such as `granola,pocket`) and a signing secret for each enabled provider.
None of these are checked in:

- **Locally:** copy `packages/worker/.dev.vars.example` to `packages/worker/.dev.vars` (gitignored) and fill it in.
- **Deployed:** set each value with `wrangler secret put`, for example
  `wrangler secret put WEBHOOK_PROVIDERS`. `WEBHOOK_PROVIDERS` is not sensitive,
  but secrets persist across deploys, whereas `wrangler deploy` resets plain vars
  to whatever `wrangler.jsonc` declares. If you prefer a dashboard var, also set
  `"keep_vars": true` in `wrangler.jsonc` and do not declare it there.

| Provider | Route | Secret binding |
| --- | --- | --- |
| `granola` | `POST /webhooks/granola` | `GRANOLA_SIGNING_SECRET` |
| `pocket` | `POST /webhooks/pocket` | `POCKET_SIGNING_SECRET` |

Configuration is read with Effect `Config` from a `ConfigProvider` over the Worker
bindings. `WEBHOOK_PROVIDERS` is a comma-separated string (names are trimmed) or,
where JSON vars are supported (a dashboard JSON var with `keep_vars`), a JSON array
such as `["granola","pocket"]`. Secrets and `.dev.vars` values are always strings,
so a JSON-looking string there is not parsed and counts as malformed. Unset or
empty `WEBHOOK_PROVIDERS` accepts nothing; a trailing comma is malformed. A provider that is unregistered or not
listed returns 404 `not_found`. A listed provider whose secret is missing, empty or
malformed returns sanitized 500 on that provider's route only. A
`WEBHOOK_PROVIDERS` value naming an unknown provider returns sanitized 500 on every
webhook route.

To add a provider, implement a `Webhook.Ingestion` builder (see `packages/worker/src/Provider/Granola.ts`)
and register it with its secret binding in `packages/worker/src/Webhook/WebhookProviders.ts`.

## Providers

`POST /webhooks/granola` verifies Standard Webhooks HMAC-SHA256 signatures over
bounded raw bytes before validating and enqueueing metadata.
`GRANOLA_SIGNING_SECRET` is Granola's `whsec_`-prefixed base64 secret. The key is
imported when the provider is resolved. `GET /` retains the template greeting. Real Granola delivery interoperability
still needs verification before ongoing ingestion is enabled.

`POST /webhooks/pocket` verifies `X-HeyPocket-Signature` (hex HMAC-SHA256 over
`{X-HeyPocket-Timestamp}.{rawBody}`, keyed with the secret string as-is) and
rejects timestamps more than 300 s (header is in milliseconds) from now. Configure
`POCKET_SIGNING_SECRET` with the secret Pocket shows when the webhook is created
or rotated. Unsigned deliveries, including those from legacy webhooks without a
secret, are rejected. Pocket bodies include the full transcript, so the read limit
is 2 MiB (Granola: 64 KiB); only `event`, `timestamp` and `recording.id` are
decoded. Pocket sends no event ID, so `eventId` is the SHA-256 hex of
`event`, `recording.id` and the payload `timestamp`. Real Pocket delivery
interoperability (signature format, retry behaviour) still needs verification.

## Local verification

```sh
bun install
bun run cf-typegen
bun run typecheck
bun run test
(cd packages/worker && bunx wrangler deploy --dry-run)
bun run dev
```

In another terminal:

```sh
curl -i http://localhost:8787/
curl -i -X POST http://localhost:8787/webhooks/granola \
  -H 'Content-Type: application/json' --data '{"synthetic":true}'
```

Create `packages/worker/.dev.vars` from `.dev.vars.example` first. Expect 200 for the root and,
for the unsigned webhook, 401 when `granola` is enabled with a secret, 404 when it is not enabled, and 500 when it is enabled
without a secret. Wrangler uses a local queue;
these checks do not require remote provisioning. Regenerate
`packages/worker/worker-configuration.d.ts` with `bun run cf-typegen` whenever Wrangler
configuration changes; it ignores local `.env` files so their variable names are
not written into the committed types.

## Structure

The repo is a Bun workspace. Dependency versions are pinned once in the root
`package.json` catalog, and `bun run typecheck` runs `tsc -b` over each package as
its own TypeScript project, typed only for the platform it runs on.

| Package | What |
| --- | --- |
| `packages/envelope` | The event envelope schema the Worker enqueues and consumers read. Platform-free. |
| `packages/worker` | The Cloudflare Worker: webhook verification, validation, and enqueueing. |

Within a package, code is organized by domain under `src/<Domain>/`. Each module is
imported as a namespace (`import * as Webhook from './Webhook/Webhook'`), so names
inside it stay short (`Webhook.Unauthorized`, `EventQueue.layer`). Tests mirror `src/`.

In `packages/envelope`:

- `src/EventEnvelope.ts`: bounded metadata schema/type, `SourceTimestamp`, and the
  strict `decode`. `src/index.ts` exports it as the `EventEnvelope` namespace.

In `packages/worker`:

- `src/Event/EventQueue.ts`: the `EventQueue` service, its `layer(binding)`
  Cloudflare producer, and typed `EnqueueFailed`.
- `src/Webhook/Webhook.ts`: provider-neutral ingestion and resolution errors, the
  `Ingestion` shape, and bounded body reading.
- `src/Webhook/WebhookProviders.ts`: the provider registry and the
  `WebhookProviders` service that resolves a provider name to its ingestion. Its
  `layer` reads `WEBHOOK_PROVIDERS` and each enabled provider's redacted secret
  through Effect `Config`, failing with `ConfigError` on a malformed list.
- `src/Webhook/WebhookHandler.ts`: `handle`, the resolve → ingest → enqueue
  orchestration and sanitized HTTP outcomes shared by every provider; no layer wiring.
- `src/Provider/Granola.ts`, `src/Provider/Pocket.ts`: `ingestion` builders taking
  a redacted secret, signature verification, and payload schemas.
- `src/Layers.ts`: `Bindings` and `AppLayer` (both service layers with a
  `ConfigProvider` over the bindings); the only place that wires layers.
- `src/index.ts`: typed Hono app factory and the Effect execution boundary.

The envelope contains only version, provider, event ID/type, source record ID,
and UTC source timestamp. Identifiers are 1–512 characters; timestamps use
`YYYY-MM-DDTHH:mm:ssZ` or millisecond precision. No meeting content is forwarded.
The ingestion contract requires authenticated, validated metadata; the queue
adapter explicitly selects fields rather than forwarding arbitrary objects.

The handler depends only on the `WebhookProviders` and `EventQueue` services.
`createApp` takes one function from bindings to the layer that provides them; the
production export passes `AppLayer`, which always uses the verifying providers.
Tests pass fixture `WebhookProviders` layers through the same boundary, with
recording or failing queue bindings; fixtures do not require live configuration.
This seam has no environment flag or HTTP bypass.

Tests use Vitest. Effect-level tests (ingestion, providers, handler, queue) run
with `@effect/vitest` `it.effect`, fixing time and driving the ingestion timeout
with `TestClock`; provider tests build config with `ConfigProvider.fromUnknown`.
Tests through `app.request` stay plain async tests and exercise the HTTP contract
against the real Worker entry point.

HTTP 202 means the queue send completed;
queue failure returns 503 `enqueue_failed`, and unexpected failures return 500
`internal_error`. No detached enqueue, automatic retries, raw payload logging,
or raw exception responses are used. Duplicate delivery remains possible.
Authentication/freshness failures return 401 `unauthorized`; invalid authenticated
payloads return 400 `invalid_payload`; oversized bodies return 413 `body_too_large`.

Granola policy: 64 KiB streamed body limit, 512-character alphanumeric/underscore/hyphen
event IDs, 12-digit delivery timestamps, 4096-character signature headers, at most
16 space-separated signatures, and an inclusive ±300-second delivery window using
Effect Clock. For every provider, ingestion times out after 10 seconds and the
request Effect is bounded to 12 seconds including enqueue. Source timestamps use the envelope's strict UTC
format and are not subject to the freshness window. All three documented event
types are accepted; edited events require `data.changed_fields: ["summary"]`.
Unknown payload fields are stripped; unknown event types are rejected.
See `spec/granola.md` for the provider contract and deployment checklist.

## Before enabling ingestion

- Verify Granola UI test deliveries and real meeting events against the documented
  contract and local validation policy; synthetic tests do not prove interoperability.
- Provision `hermes-webhook-events` for the `EVENTS` producer binding. Configure
  HTTP pull consumption and verify free-plan 24-hour retention separately.
- Set `WEBHOOK_PROVIDERS` and each enabled provider's signing secret with
  `wrangler secret put`, not in source control.
- Implement the private Hermes consumer with durable deduplication and safe
  acknowledgment, outside this repository.
- Verify real events and deployed CPU limits. Dry-run bundle size alone does not
  establish free-plan CPU compliance.

No remote resources were created or deployed by this foundation setup.
`bun run deploy` is available only for a later, explicitly authorized deployment.
