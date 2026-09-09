import { transaction } from "../src/db.js"
import { bootstrap, shutdown } from "../src/index.js"
import { sweepExpiredHolds } from "../src/commands/holds/sweepHolds.js"

await bootstrap()

const results = await sweepExpiredHolds(transaction)

for (const r of results) {
  console.log(
    r.outcome === "skipped"
      ? `[SWEEP] skipped ${r.holdId}: ${r.error}`
      : `[SWEEP] ${r.outcome} ${r.holdId}`
  )
}

console.log(`[SWEEP] done — ${results.length} hold(s)`)

await shutdown()
