import { Hono } from 'hono'
import { Effect } from 'effect'
import { AppLayer, type Bindings, type MakeAppLayer } from './Layers'
import * as WebhookHandler from './Webhook/WebhookHandler'

// Ingestion has its own 10 s timeout (INGESTION_TIMEOUT); this outer bound also
// covers the queue send, which has none, and leaves headroom for the inner one to fire first.
const REQUEST_TIMEOUT = '12 seconds'

export const createApp = (makeLayer: MakeAppLayer) => {
  const app = new Hono<{ Bindings: Bindings }>()
  app.get('/', (c) => c.text('Hello Hono!'))
  app.post('/webhooks/:provider', (c) =>
    Effect.runPromise(WebhookHandler.handle(c.req.param('provider'))(c.req.raw).pipe(
      Effect.provide(makeLayer(c.env)),
      Effect.timeout(REQUEST_TIMEOUT),
      Effect.catchCause(() => Effect.succeed(c.json({ error: 'internal_error' }, 500))),
    )))
  app.onError((_error, c) => c.json({ error: 'internal_error' }, 500))
  return app
}

export default createApp(AppLayer)
