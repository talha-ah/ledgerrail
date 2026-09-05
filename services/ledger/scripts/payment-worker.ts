import { transaction } from "../src/db.js"
import { bootstrap, shutdown } from "../src/index.js"
import { createFakeRail } from "../src/rails/index.js"
import { runWorker } from "../src/payments/orchestrator.js"

const controller = new AbortController()

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => controller.abort())
}

await bootstrap()

// The fake until 3.0 puts a socket here. Nothing above this line knows which.
await runWorker(
  { rail: createFakeRail(), transaction },
  { signal: controller.signal }
)

await shutdown()
