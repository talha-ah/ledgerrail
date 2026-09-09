import type { PoolClient } from "pg"

import type { Hold } from "../../types.js"
import { errorMessage } from "../../utils/error.js"
import { holdKey, releaseHold } from "./settleHold.js"

type Transactor = <T>(cb: (client: PoolClient) => Promise<T>) => Promise<T>

export type SweepResult =
  | { holdId: string; outcome: "released" | "replayed" }
  | { holdId: string; outcome: "skipped"; error: string }

/**
 * Expired holds whose payment has finished with them.
 *
 * `expires_at` alone is not enough, and the `NOT EXISTS` is the whole safety
 * argument. HOLD_TTL_MS is a guess about how long a saga takes, and a guess that
 * is wrong once releases money a payment is still trying to send. A payment that
 * has not reached a terminal state owns its hold no matter what the clock says —
 * and one that has, including one whose compensation gave up and went to
 * `failed`, does not. That second half is what makes `failed` a safe terminal
 * for a compensation at all.
 *
 * SKIP LOCKED for `claimPayments`' reason: two sweepers cooperate rather than
 * queue. It is an efficiency, not the correctness argument — the lock is gone by
 * the time the release runs, and what makes a double release harmless is that
 * both callers derive the same key.
 */
async function claimExpired(
  client: PoolClient,
  limit: number
): Promise<Hold[]> {
  const { rows } = await client.query<Hold>(
    `
    SELECT h.* FROM holds h
     WHERE h.status = 'held'
       AND h.expires_at < NOW()
       AND NOT EXISTS (
             SELECT 1 FROM payments p
              WHERE p.hold_id = h.hold_id
                AND p.state NOT IN ('completed', 'compensated', 'failed')
           )
     ORDER BY h.expires_at
     LIMIT $1
     FOR UPDATE OF h SKIP LOCKED;
    `,
    [limit]
  )

  return rows
}

export async function sweepExpiredHolds(
  transaction: Transactor,
  limit = 100
): Promise<SweepResult[]> {
  const claimed = await transaction((client) => claimExpired(client, limit))

  const results: SweepResult[] = []

  // One transaction per hold, not one for the batch. A sweep is a sequence of
  // independent releases; batching them means one unreleasable hold rolls back
  // every good release beside it, and the next pass finds the same bad hold at
  // the front of the queue again. Caught per hold for the same reason.
  for (const hold of claimed) {
    try {
      const receipt = await transaction((client) =>
        releaseHold(hold.hold_id, holdKey(hold.hold_id, "release"), client)
      )

      results.push({
        holdId: hold.hold_id,
        outcome: receipt.replayed ? "replayed" : "released",
      })
    } catch (err) {
      results.push({
        holdId: hold.hold_id,
        outcome: "skipped",
        error: errorMessage(err),
      })
    }
  }

  return results
}
