import type { Pool, PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import { getTestPool } from "../../../test/db.js"
import { randomString } from "../../utils/helpers.js"
import type { Event, Balance, BalanceSnapshot } from "../../types.js"
import {
  applyToBalances,
  rebuildBalances,
  balancesSnapshot,
} from "./balances.js"
import {
  postTransaction,
  type PostTransactionInput,
} from "../../commands/postTransaction/index.js"

async function createAccounts(pool: Pool): Promise<{ a: number; s: number }> {
  const s = await pool.query(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'system') RETURNING id;`,
    [randomString()]
  )
  const a = await pool.query(
    `INSERT INTO accounts (account_number, currency, type) VALUES ($1, 'PKR', 'asset') RETURNING id;`,
    [randomString()]
  )

  const assetId: number = a.rows[0].id
  const systemId: number = s.rows[0].id

  return { a: assetId, s: systemId }
}

async function postTransactions(
  assetId: number,
  systemId: number,
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

async function rebuildBalancesFn(client: PoolClient) {
  try {
    await client.query("BEGIN")
    await rebuildBalances(client)
    await client.query("COMMIT")
  } catch (err) {
    await client.query("ROLLBACK")
    throw err
  }
}

describe("Apply balance", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const { a, s } = await createAccounts(pool)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("Replay events", async () => {
    const event: Pick<Event, "global_seq" | "created_at" | "payload"> = {
      global_seq: 100n,
      created_at: new Date(),
      payload: {
        idempotencyKey: randomString(),
        currency: "PKR",
        postings: [
          {
            accountId: systemId,
            amount: "-100",
          },
          {
            accountId: assetId,
            amount: "100",
          },
        ],
      },
    }

    try {
      await client.query("BEGIN")
      await applyToBalances(event, client)
      await client.query("COMMIT")
    } catch (err) {
      await client.query("ROLLBACK")
      throw err
    }

    const { rows: balances } = await client.query<Balance>(
      "SELECT * FROM balances WHERE last_event_seq = $1",
      [event.global_seq]
    )

    const assetBalance = balances.find((b) => b.account_id === assetId)
    const systemBalance = balances.find((b) => b.account_id === systemId)

    expect(assetBalance?.amount).toBe("100")
    expect(systemBalance?.amount).toBe("-100")
  })
})

describe("Balances Rebuild", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const { a, s } = await createAccounts(pool)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("Replay events", async () => {
    await postTransactions(assetId, systemId, client)

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances WHERE account_id = ANY($1)",
      [[systemId, assetId]]
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }

    await rebuildBalancesFn(client)

    const { rows: afterBalances } = await client.query<Balance>(
      "SELECT * FROM balances WHERE account_id = ANY($1);",
      [[systemId, assetId]]
    )

    expect(beforeBalances.length).toBe(afterBalances.length)

    for (const balance of afterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }
  })
})

describe("Balances Snapshot", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const { a, s } = await createAccounts(pool)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("Take a snapshot", async () => {
    async function takeSnapshot(client: PoolClient): Promise<void> {
      try {
        await client.query("BEGIN")
        await balancesSnapshot(client)
        await client.query("COMMIT")
      } catch (err) {
        await client.query("ROLLBACK")
        throw err
      }
    }

    await postTransactions(assetId, systemId, client)
    await takeSnapshot(client)

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )
    const { rows: beforeSnapshots } = await client.query<BalanceSnapshot>(
      "SELECT * FROM balance_snapshots"
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-815n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(815n)
      }
    }

    for (const snapshot of beforeSnapshots) {
      if (snapshot.account_id === systemId) {
        expect(BigInt(snapshot.amount)).toBe(-815n)
      } else if (snapshot.account_id === assetId) {
        expect(BigInt(snapshot.amount)).toBe(815n)
      }
    }

    await postTransactions(assetId, systemId, client)

    const { rows: afterBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )
    const { rows: afterSnapshots } = await client.query<BalanceSnapshot>(
      `
      SELECT DISTINCT ON (account_id) account_id, amount, as_of_event_seq
      FROM balance_snapshots
      ORDER BY account_id, as_of_event_seq DESC;
      `
    )

    for (const balance of afterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-1630n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(1630n)
      }
    }
    for (const snapshot of afterSnapshots) {
      if (snapshot.account_id === systemId) {
        expect(BigInt(snapshot.amount)).toBe(-815n)
      } else if (snapshot.account_id === assetId) {
        expect(BigInt(snapshot.amount)).toBe(815n)
      }
    }

    await rebuildBalancesFn(client)

    const { rows: afterAfterBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    for (const balance of afterAfterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-1630n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(1630n)
      }
    }
  })
})

describe("rebuildBalances — snapshots at differing cursors", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let assetId2: number
  let systemId: number
  let systemId2: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const pair1 = await createAccounts(pool)
    assetId = pair1.a
    systemId = pair1.s

    const pair2 = await createAccounts(pool)
    assetId2 = pair2.a
    systemId2 = pair2.s
  })

  afterAll(async () => {
    client.release()
    await pool?.end()
  })

  it("restores every account, not only those at the latest snapshot cursor", async () => {
    const accountIds = [systemId, assetId, systemId2, assetId2]

    const readBalances = async () => {
      const { rows } = await client.query<Balance>(
        `SELECT account_id, amount FROM balances
         WHERE account_id = ANY($1) ORDER BY account_id`,
        [accountIds]
      )
      return rows
    }

    // Pair 1 transacts, then pair 2 — so the two pairs end at different event sequences.
    await postTransactions(assetId, systemId, client)
    await postTransactions(assetId2, systemId2, client)

    await client.query("BEGIN")
    await balancesSnapshot(client)
    await client.query("COMMIT")

    // More activity on pair 2 only, so the rebuild must replay past the snapshot.
    await postTransactions(assetId2, systemId2, client)

    const before = await readBalances()
    expect(before).toHaveLength(4)

    await rebuildBalancesFn(client)

    const after = await readBalances()

    // Cardinality first: a dropped account is invisible to a Σ=0 check,
    // because the surviving accounts still net to zero.
    expect(after).toHaveLength(before.length)
    expect(after).toEqual(before)
  })
})
