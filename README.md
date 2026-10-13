# hermes-webhook-relay

A Hono Cloudflare Worker with Effect v4 webhook ingestion and queueing for
Granola and Pocket, and `relay-consumer`, a CLI that pulls those events into a
local inbox for an agent such as Hermes to work through.

```text
Granola / Pocket ──signed webhook──▶ Worker ──▶ Cloudflare Queue
                                                     │  HTTP pull (outbound)
                                                     ▼
                              relay-consumer pull ──▶ local inbox ◀── pending / show / complete
```

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

## Consumer

`packages/consumer` builds `relay-consumer`, one self-contained binary (Bun's
runtime included, so the host doesn't need Bun):

```sh
bun run build                     # → packages/consumer/dist/relay-consumer
```

| Command | What |
| --- | --- |
| `pull` | Moves new events from the queue into the inbox, and acknowledges them once stored. |
| `pending [--limit N] [--json]` | Events not yet completed, oldest first: `provider:eventId eventType sourceRecordId sourceTimestamp`. |
| `show <ref> [--transcript]` | The event and the current state of its provider record (Granola note, Pocket recording), as JSON. Transcripts are left out unless `--transcript` is given; then the whole transcript is included, however long (Granola's is read page by page). |
| `complete <ref>` | Marks an event handled; `pending` stops listing it. Completing it again is fine. |
| `quarantine` | Queue messages that could never be events, as JSON. |

`<ref>` is `provider:eventId`, as `pending` prints it. Errors are one line on
stderr and exit 1.

**Settings** are read from the environment, then from a dotenv file
(`~/.config/hermes-webhook-relay/env`, or `RELAY_ENV_FILE`); see
`packages/consumer/env.example`. Only `pull` needs the Cloudflare settings and
only `show` needs a provider API key, so an agent that only lists, reads and
completes events never needs the queue token. The inbox is SQLite at
`~/.local/state/hermes-webhook-relay/inbox.sqlite` (or `RELAY_INBOX`).

**Queue setup.** Enable HTTP pull on the queue
(`wrangler queues consumer http add hermes-webhook-events --message-retries 100`)
and create an API token with Account → Queues → Edit, which pulling and
acknowledging both need.

**Delivery guarantees.** `pull` commits each batch to the inbox (`synchronous =
FULL`) before acknowledging it; if storing fails nothing is acknowledged and
Cloudflare redelivers the batch after a 60-second lease. Events are deduplicated
by provider and event ID, including ones already completed, so redelivery is
harmless. Other messages are handled so none is silently lost:

- Malformed (not a valid envelope), or a different event reusing a stored event
  ID: kept in the inbox's quarantine with its body, then acknowledged.
  `pending` adds a `quarantined N` line while any are kept.
- A valid header with an envelope version or provider this binary doesn't know
  (the Worker was upgraded first): not acknowledged, but retried with a one-hour
  delay. With `--message-retries 100` it outlasts the queue's 24-hour retention,
  so upgrading `relay-consumer` within a day recovers it.

The inbox keeps only event metadata. `show` fetches meeting content on demand and
nothing stores it.

### Hermes

Run it from a Hermes cron job in monitor mode: a script runs every tick, and the
agent runs only when the script's output changes.

1. Put the binary at `~/.local/bin/relay-consumer` (or set `RELAY_CONSUMER` in the
   script) and the settings file at `~/.config/hermes-webhook-relay/env`, `chmod 600`.
   Hermes strips credentials from scripts' environment, so the file is where they go.
2. Copy `packages/consumer/hermes/relay_monitor.sh` to `~/.hermes/scripts/`. It runs
   `pull`, then `pending`. A failed pull exits non-zero, which Hermes reports as an alert.
3. Create the job, adjusting the prompt to what the agent should do with meetings:

   ```sh
   hermes cron create '*/5 * * * *' \
     'New meeting events are pending (provider:eventId type record timestamp). For each, run
     `relay-consumer show <ref>` to read it (summary, attendees, calendar event), act on it, then
     `relay-consumer complete <ref>`. Add `--transcript` only when the summary is not enough: a
     transcript can be very long. Leave an event pending if you could not finish it. If nothing
     is pending, reply [SILENT].' \
     --name 'meeting inbox' --monitor-script relay_monitor.sh
   ```

The agent needs a terminal that can run `relay-consumer`. Completing events
changes `pending`, so the next tick wakes the agent once more with nothing new;
the prompt's `[SILENT]` covers it. An event left pending doesn't wake the agent
again until something else in the list changes.

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
| `packages/consumer` | `relay-consumer`, the Bun CLI that pulls events into a local inbox. |

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

In `packages/consumer`:

- `src/Queue/QueueClient.ts`: the Cloudflare Queues HTTP pull client (`pull`,
  `ack` with retries), retrying transient failures, and branded `MessageId` / `LeaseId`.
- `src/Inbox/Inbox.ts`: the SQLite inbox (events and quarantine) over a generic
  `SqlClient`. Rows decode with the envelope's branded schemas; `RefFromString` is
  the `provider:eventId` codec the CLI parses arguments with.
- `src/Pull/Pull.ts`: classifying queue messages and the store-then-acknowledge loop.
- `src/SourceRecord/SourceRecord.ts`: the provider-neutral `Reader` shape `show`
  uses and its errors (the consumer's counterpart of `Webhook.Ingestion`).
- `src/SourceRecord/SourceRecordProviders.ts`: the registry (each provider's API key
  variable and `reader` builder) and the `SourceRecordProviders` service that
  resolves a provider to its reader, failing with `NotConfigured` without a key.
- `src/Provider/Granola.ts`, `src/Provider/Pocket.ts`: `reader` builders taking a
  redacted API key.
- `src/Config.ts`, `src/Layers.ts`, `src/Cli.ts`, `src/main.ts`: settings and the
  env file, layer wiring, the commands, and the Bun entry point.
- `src/testing/`: an in-memory Cloudflare Queues API and inbox for tests.

The binary uses `@effect/sql-sqlite-bun`. Tests run on Node under Vitest, so they
give the inbox `@effect/sql-sqlite-node` instead; Bun's `node:sqlite` can't stand in
for `bun:sqlite` because on macOS it loads a system SQLite built without extension
loading, which `@effect/sql-sqlite-node` requires.

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
- Enable HTTP pull with `--message-retries 100` and set up `relay-consumer`
  (see [Consumer](#consumer)).
- Verify real events and deployed CPU limits. Dry-run bundle size alone does not
  establish free-plan CPU compliance.

No remote resources were created or deployed by this foundation setup.
`bun run deploy` is available only for a later, explicitly authorized deployment.
