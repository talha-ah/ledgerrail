import type { PoolClient } from "pg"

import { randomString } from "../src/utils/helpers.js"
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

export async function postTransactions(
  systemId: number,
  assetId: number,
  client: PoolClient
): Promise<void> {
  const TRANSACTIONS: bigint[] = [20n, 100n, 70n, -100n, 200n, 10n, 500n, 15n]

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
      await postTransaction(input, client)
      await client.query("COMMIT")
    } catch (err) {
      await client.query("ROLLBACK")
      throw err
    }
  }
}
