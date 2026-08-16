import type { PoolClient } from "pg"

import type { Event } from "../src/types.js"
import { getTestPool, testTransaction } from "./db.js"
import { randomString } from "../src/utils/helpers.js"
import { TOPICS, GROUPS, transactionKey } from "../src/kafka/utils.js"
import { makeMessageHandler } from "../src/projections/balanceProjection/consumer.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../src/commands/postTransaction/index.js"

export async function createAccounts(
  client: PoolClient
): Promise<{ a: number; s: number }> {
  const s = await client.query(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'system') RETURNING id;`,
    [randomString()]
  )
  const a = await client.query(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'asset') RETURNING id;`,
    [randomString()]
  )

  const assetId: number = a.rows[0].id
  const systemId: number = s.rows[0].id

  return { a: assetId, s: systemId }
}

export async function projectEvents(eventIds: string[]): Promise<void> {
  if (eventIds.length === 0) return

  const { rows } = await getTestPool().query<Event>(
    `SELECT global_seq, event_id, event_type, event_version, payload, created_at
       FROM events
      WHERE event_id = ANY($1)
      ORDER BY global_seq`,
    [eventIds]
  )

  if (rows.length !== eventIds.length) {
    throw new Error(
      `projectEvents: expected ${eventIds.length} events, found ${rows.length}`
    )
  }

  const onMessage = makeMessageHandler(
    testTransaction,
    GROUPS.BALANCES_PROJECTION
  )

  for (const [i, event] of rows.entries()) {
    await onMessage({
      partition: 0,
      offset: String(i),
      topic: TOPICS.LEDGER_EVENTS,
      key: transactionKey(event.event_id),
      value: {
        payload: event.payload,
        event_id: event.event_id,
        event_type: event.event_type,
        event_version: event.event_version,
        global_seq: String(event.global_seq),
        created_at: event.created_at.toISOString(),
      },
    })
  }
}

export async function postTransactions(
  systemId: number,
  assetId: number,
  client: PoolClient
): Promise<{ globalSeq: string; eventId: string }[]> {
  const TRANSACTIONS: bigint[] = [20n, 100n, 70n, 200n, 10n, 500n, 15n]

  const transactions: { globalSeq: string; eventId: string }[] = []

  for (const transaction of TRANSACTIONS) {
    const input: PostTransactionInput = {
      idempotencyKey: randomString(),
      currency: "PKR",
      postings: [
        {
          accountId: systemId,
          amount: BigInt(-transaction),
        },
        {
          accountId: assetId,
          amount: BigInt(transaction),
        },
      ],
    }

    try {
      await client.query("BEGIN")
      transactions.push(await postTransaction(input, client))
      await client.query("COMMIT")
    } catch (err) {
      await client.query("ROLLBACK")
      throw err
    }
  }

  return transactions
}

export async function postAndProject(
  systemId: number,
  assetId: number,
  client: PoolClient
) {
  const transactions = await postTransactions(systemId, assetId, client)

  await projectEvents(transactions.map((r) => r.eventId))

  return transactions
}
