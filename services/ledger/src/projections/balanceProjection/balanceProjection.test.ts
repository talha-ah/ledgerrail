import type { Pool, PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import { randomString } from "../../utils/helpers.js"
import type { EventEnvelope } from "../../kafka/types.js"
import { getTestPool, closeTestPool } from "../../../test/db.js"
import type {
  Event,
  Balance,
  BalanceSnapshot,
  TransactionPostedPayloadV2,
} from "../../types.js"
import { createAccounts, postAndProject } from "../../../test/utils.js"
import {
  applyToBalances,
  rebuildBalances,
  balancesSnapshot,
} from "./balanceProjection.js"

afterAll(closeTestPool)

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

    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
  })

  it("Replay events", async () => {
    const event: Pick<
      EventEnvelope,
      "event_version" | "global_seq" | "created_at" | "payload"
    > = {
      event_version: 1,
      global_seq: String(100n),
      created_at: new Date().toISOString(),
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

    expect(assetBalance?.amount).toBe(100n)
    expect(systemBalance?.amount).toBe(-100n)
  })
})

describe("Balances Rebuild", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
  })

  it("Replay events", async () => {
    await postAndProject(systemId, assetId, client)

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances WHERE account_id = ANY($1)",
      [[systemId, assetId]]
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-915n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(915n)
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
        expect(BigInt(balance.amount)).toBe(-915n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(915n)
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

    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterAll(async () => {
    client.release()
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

    await postAndProject(systemId, assetId, client)
    await takeSnapshot(client)

    const { rows: beforeBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )
    const { rows: beforeSnapshots } = await client.query<BalanceSnapshot>(
      "SELECT * FROM balance_snapshots"
    )

    for (const balance of beforeBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-915n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(915n)
      }
    }

    for (const snapshot of beforeSnapshots) {
      if (snapshot.account_id === systemId) {
        expect(BigInt(snapshot.amount)).toBe(-915n)
      } else if (snapshot.account_id === assetId) {
        expect(BigInt(snapshot.amount)).toBe(915n)
      }
    }

    await postAndProject(systemId, assetId, client)

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
        expect(BigInt(balance.amount)).toBe(-1830n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(1830n)
      }
    }
    for (const snapshot of afterSnapshots) {
      if (snapshot.account_id === systemId) {
        expect(BigInt(snapshot.amount)).toBe(-915n)
      } else if (snapshot.account_id === assetId) {
        expect(BigInt(snapshot.amount)).toBe(915n)
      }
    }

    await rebuildBalancesFn(client)

    const { rows: afterAfterBalances } = await client.query<Balance>(
      "SELECT * FROM balances"
    )

    for (const balance of afterAfterBalances) {
      if (balance.account_id === systemId) {
        expect(BigInt(balance.amount)).toBe(-1830n)
      } else if (balance.account_id === assetId) {
        expect(BigInt(balance.amount)).toBe(1830n)
      }
    }
  })
})

describe("Rebuild Balances — snapshots at differing cursors", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let assetId2: number
  let systemId: number
  let systemId2: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const pair1 = await createAccounts(client)
    assetId = pair1.a
    systemId = pair1.s

    const pair2 = await createAccounts(client)
    assetId2 = pair2.a
    systemId2 = pair2.s
  })

  afterAll(async () => {
    client.release()
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
    await postAndProject(systemId, assetId, client)
    await postAndProject(systemId2, assetId2, client)

    await client.query("BEGIN")
    await balancesSnapshot(client)
    await client.query("COMMIT")

    // More activity on pair 2 only, so the rebuild must replay past the snapshot.
    await postAndProject(systemId2, assetId2, client)

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

describe("Rebuild across an event-version boundary", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()
    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterAll(async () => client.release())

  async function journal(version: 1 | 2, payload: unknown) {
    const { rows } = await client.query<
      Pick<Event, "global_seq" | "created_at">
    >(
      `INSERT INTO events (event_type, event_version, payload, idempotency_key)
      VALUES ('TransactionPosted', $1, $2, $3)
      RETURNING global_seq, created_at;
      `,
      [
        version,
        JSON.stringify(payload),
        (payload as TransactionPostedPayloadV2).idempotencyKey,
      ]
    )

    const row = rows[0]
    if (!row) throw new Error("failed to insert fixture event")

    await applyToBalances(
      {
        payload: payload,
        event_version: version,
        global_seq: String(row.global_seq),
        created_at: row.created_at.toISOString(),
      },
      client
    )
  }

  it("reproduces balances from a journal holding both versions", async () => {
    await journal(1, {
      currency: "PKR",
      idempotencyKey: randomString(),
      postings: [
        { accountId: systemId, amount: "-100" },
        { accountId: assetId, amount: "100" },
      ],
    })

    await journal(2, {
      idempotencyKey: randomString(),
      postings: [
        { accountId: systemId, amount: "-50", currency: "PKR" },
        { accountId: assetId, amount: "50", currency: "PKR" },
      ],
    })

    const { rows: before } = await client.query<Balance>(
      `SELECT * FROM balances WHERE account_id = ANY($1) ORDER BY account_id`,
      [[systemId, assetId]]
    )

    expect(before).toHaveLength(2)
    expect(before.find((b) => b.account_id === assetId)?.amount).toBe(150n)

    await rebuildBalancesFn(client)

    const { rows: after } = await client.query<Balance>(
      `SELECT * FROM balances WHERE account_id = ANY($1) ORDER BY account_id`,
      [[systemId, assetId]]
    )

    // The claim: a replay from the journal reproduces the live projection
    // exactly. It can only hold if the rebuild upcasts too — which is the whole
    // reason the upcast sits inside applyToBalances rather than at the callers.
    expect(after).toEqual(before)
  })
})
