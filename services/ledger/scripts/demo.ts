import type { Pool, PoolClient } from "pg"

import { formatMoney } from "@ledgerrail/money"

import { bootstrap } from "../src/index.js"
import type { Balance } from "../src/types.js"
import { getPool, transaction } from "../src/db.js"
import { randomString } from "../src/utils/helpers.js"
import { balancesSnapshot } from "../src/projections/balanceProjection/index.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../src/commands/postTransaction/index.js"

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

  console.log("Wiping balances")
  await pool.query("DELETE FROM balances WHERE account_id = ANY($1)", [
    [systemId, asset1Id, asset2Id],
  ])

  await getBalances(systemId, asset1Id, asset2Id, pool)

  await pool.end()
}

main()
