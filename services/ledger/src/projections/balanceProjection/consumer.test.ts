import { randomUUID } from "crypto"
import type { Pool, PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import { makeMessageHandler } from "./consumer.js"
import { randomString } from "../../utils/helpers.js"
import { createAccounts } from "../../../test/utils.js"
import { TOPICS, transactionKey } from "../../kafka/index.js"
import type { EventEnvelope, IncomingMessage } from "../../kafka/index.js"
import {
  getTestPool,
  closeTestPool,
  testTransaction,
} from "../../../test/db.js"

afterAll(closeTestPool)

describe("Balances consumer", () => {
  const pool: Pool = getTestPool()

  let seq = 1000n
  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()
    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterAll(() => client?.release())

  const envelope = (amount: bigint): EventEnvelope => ({
    event_id: randomUUID(),
    global_seq: String(seq++),
    event_type: "TransactionPosted",
    event_version: 1,
    created_at: new Date().toISOString(),
    payload: {
      currency: "PKR",
      idempotencyKey: randomString(),
      postings: [
        { accountId: systemId, amount: String(-amount) },
        { accountId: assetId, amount: String(amount) },
      ],
    },
  })

  const msg = (value: IncomingMessage["value"]): IncomingMessage => ({
    topic: TOPICS.LEDGER_EVENTS,
    partition: 0,
    offset: "0",
    key: transactionKey("test"),
    value,
  })

  async function balanceOf(accountId: number): Promise<bigint> {
    const { rows: balances } = await client.query<{ amount: bigint }>(
      "SELECT amount FROM balances WHERE account_id = $1",
      [accountId]
    )
    return balances[0]?.amount ?? 0n
  }

  it("applies a valid event to the project", async () => {
    const onMessage = makeMessageHandler(testTransaction)

    await onMessage(msg(envelope(100n)))

    expect(await balanceOf(assetId)).toBe(100n)
    expect(await balanceOf(systemId)).toBe(-100n)
  })

  it("skips messages that fail validation, without throwing", async () => {
    const onMessage = makeMessageHandler(testTransaction)

    const before = await balanceOf(assetId)

    await expect(onMessage(msg(null))).resolves.toBeUndefined()
    await expect(onMessage(msg("not an object"))).resolves.toBeUndefined()
    await expect(onMessage(msg({ nope: true }))).resolves.toBeUndefined()
    await expect(
      onMessage(msg({ ...envelope(50n), event_version: 99 }))
    ).resolves.toBeUndefined()
    await expect(
      onMessage(msg({ ...envelope(50n), event_type: "something else" }))
    ).resolves.toBeUndefined()

    expect(await balanceOf(assetId)).toBe(before)
  })
})
