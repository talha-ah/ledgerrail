import type { Pool, PoolClient } from "pg"

import { formatMoney } from "@ledgerrail/money"

import { bootstrap } from "../src/index.js"
import { getPool, transaction } from "../src/db.js"
import type { Event, Balance } from "../src/types.js"
import { randomString } from "../src/utils/helpers.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../src/commands/postTransaction/index.js"
import {
  applyToBalances,
  rebuildBalances,
  balancesSnapshot,
} from "../src/projections/balanceProjection/index.js"

async function createAccount(type: "system" | "asset", pool: Pool) {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', $2) RETURNING id;`,
    [randomString(), type]
  )
  if (!rows[0]?.id) throw new Error(`Account ${type} couldn't be created`)

  return rows[0].id
}

async function postTransactionFn(
  account1: number,
  account2: number,
  amount: bigint,
  idempotencyKey: string,
  client: PoolClient
) {
  const transactionInput: PostTransactionInput = {
    idempotencyKey,
    currency: "PKR",
    postings: [
      { accountId: account1, amount: -amount },
      { accountId: account2, amount: amount },
    ],
  }

  const result = await postTransaction(transactionInput, client)
  console.log(
    `Transaction: Global Seq = ${result.globalSeq} Event Id = ${result.eventId}`
  )
  return result
}

async function getBalances(
  systemId: number,
  asset1Id: number,
  asset2Id: number,
  pool: Pool
) {
  const { rows: balances } = await pool.query<Balance>(
    "SELECT * FROM balances WHERE account_id = ANY($1)",
    [[systemId, asset1Id, asset2Id]]
  )

  function logBalance(
    balances: Balance[],
    accountId: number,
    type: "System" | "Asset A" | "Asset B"
  ) {
    const balance = balances.find((b) => b.account_id === accountId)?.amount

    console.log(
      `[Balance] ${type} : ${formatMoney({ amount: balance || 0n, currency: "PKR" })} PKR`
    )
  }

  logBalance(balances, systemId, "System")
  logBalance(balances, asset1Id, "Asset A")
  logBalance(balances, asset2Id, "Asset B")

  return balances
}

/**
 * No consumer is running, so the demo owns the read side for its own events —
 * the same applyToBalances the consumer calls, over the events just written.
 * Without this, `balances` stays empty and every number below is a zero that
 * looks like a balance.
 */
async function project(client: PoolClient) {
  // last_event_seq is the resume cursor from 1.2. Reading it makes repeated
  // calls safe — a projection that applies deltas must never replay a range
  // it has already applied.
  const { rows: cursor } = await client.query<{ seq: bigint | null }>(
    "SELECT MAX(last_event_seq) AS seq FROM balances"
  )

  const { rows } = await client.query<Event>(
    `SELECT global_seq, event_version, payload, created_at
       FROM events WHERE global_seq > $1 ORDER BY global_seq`,
    [String(cursor[0]?.seq ?? 0)]
  )

  for (const event of rows) {
    await applyToBalances(
      {
        payload: event.payload,
        event_type: event.event_type,
        event_version: event.event_version,
        global_seq: String(event.global_seq),
        created_at: event.created_at.toISOString(),
      },
      client
    )
  }
}

async function main() {
  // Bootstrap application
  await bootstrap()

  const pool = getPool()

  console.log("Creating accounts")
  const systemId = await createAccount("system", pool)
  const asset1Id = await createAccount("asset", pool)
  const asset2Id = await createAccount("asset", pool)

  if (!systemId || !asset1Id || !asset2Id) {
    throw new Error("Accounts could not be created")
  }

  console.log("Funding Asset")
  await transaction((client) =>
    postTransactionFn(systemId, asset1Id, 100n, randomString(), client)
  )

  await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Making a transfer between assets")
  const idempotencyKey = randomString()
  await transaction((client) =>
    postTransactionFn(asset1Id, asset2Id, 40n, idempotencyKey, client)
  )

  await getBalances(systemId, asset1Id, asset2Id, pool)

  try {
    console.log("Attempting an overdraft")
    await transaction((client) =>
      postTransactionFn(asset1Id, asset2Id, 140n, randomString(), client)
    )
    console.log("Overdraft succeeded")
  } catch (err) {
    console.log("Overdraft rejected")
  }

  await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Replaying a duplicate idempotency key")
  await transaction((client) =>
    postTransactionFn(asset1Id, asset2Id, 10n, idempotencyKey, client)
  )
  console.log("↳ replay returned the SAME event (no new posting)")

  await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Taking a snapshot")
  await transaction((client) => balancesSnapshot(client))

  console.log("Projecting the events onto balances")
  await transaction((client) => project(client))

  console.log("Making some transactions")
  await transaction((client) =>
    postTransactionFn(asset1Id, asset2Id, 10n, randomString(), client)
  )
  await transaction((client) =>
    postTransactionFn(asset1Id, asset2Id, 10n, randomString(), client)
  )
  await transaction((client) =>
    postTransactionFn(asset1Id, asset2Id, 10n, randomString(), client)
  )

  await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Projecting the new events")
  await transaction((client) => project(client))

  // Captured before the projection is destroyed: this is what the rebuild has
  // to reproduce, and comparing against it is the point of the whole demo.
  const before = await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Wiping balances")
  await pool.query("DELETE FROM balances WHERE account_id = ANY($1)", [
    [systemId, asset1Id, asset2Id],
  ])

  await getBalances(systemId, asset1Id, asset2Id, pool)

  console.log("Rebuilding from the journal")
  await transaction((client) => rebuildBalances(client))

  const after = await getBalances(systemId, asset1Id, asset2Id, pool)

  // Amounts, not row counts: a rebuild that recomputes every balance as zero
  // keeps the cardinality identical and tells you nothing.
  const fingerprint = (rows: Balance[]) =>
    [...rows]
      .sort((a, b) => a.account_id - b.account_id)
      .map((b) => `${b.account_id}:${b.amount}`)
      .join("  ")

  if (fingerprint(before) !== fingerprint(after)) {
    throw new Error(
      `rebuild did not reproduce the projection\n  before  ${fingerprint(before)}\n  after   ${fingerprint(after)}`
    )
  }

  console.log("↳ rebuilt from the journal — identical numbers")

  await pool.end()
}

main()
