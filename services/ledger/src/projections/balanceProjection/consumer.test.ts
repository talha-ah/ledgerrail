import type { Pool, PoolClient } from "pg"
import { describe, it, beforeEach, afterEach, expect } from "vitest"

import { makeMessageHandler } from "./consumer.js"
import { randomString } from "../../utils/helpers.js"
import { createAccounts } from "../../../test/utils.js"
import { TOPICS, transactionKey } from "../../kafka/index.js"
import { getTestPool, testTransaction } from "../../../test/db.js"
import type { Event, TransactionPostedPayload } from "../../types.js"
import type { EventEnvelope, IncomingMessage } from "../../kafka/index.js"

describe("Balances consumer", () => {
  const pool: Pool = getTestPool()

  let assetId: number
  let systemId: number
  let client: PoolClient

  beforeEach(async () => {
    client = await pool.connect()
    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
  })

  afterEach(() => client?.release())

  const envelope = async (amount: bigint): Promise<EventEnvelope> => {
    const payload: TransactionPostedPayload = {
      currency: "PKR",
      idempotencyKey: randomString(),
      postings: [
        { accountId: systemId, amount: String(-amount) },
        { accountId: assetId, amount: String(amount) },
      ],
    }

    const { rows } = await client.query<
      Pick<Event, "event_id" | "global_seq" | "created_at">
    >(
      `
      INSERT INTO events (event_type, event_version, payload, idempotency_key)
      VALUES ('TransactionPosted', 1, $1, $2)
      RETURNING event_id, global_seq, created_at;
    `,
      [JSON.stringify(payload), payload.idempotencyKey]
    )

    const row = rows[0]
    if (!row) throw new Error("failed to insert fixture event")

    return {
      event_version: 1,
      event_id: row.event_id,
      event_type: "TransactionPosted",
      global_seq: String(row.global_seq),
      created_at: row.created_at.toISOString(),
      payload,
    }
  }

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
    const onMessage = makeMessageHandler(testTransaction, "group-a")

    const env = await envelope(100n)
    await onMessage(msg(env))

    expect(await balanceOf(assetId)).toBe(100n)
    expect(await balanceOf(systemId)).toBe(-100n)
  })

  it("skips messages that fail validation, without throwing", async () => {
    const onMessage = makeMessageHandler(testTransaction, "group-a")

    const before = await balanceOf(assetId)

    await expect(onMessage(msg(null))).resolves.toBeUndefined()
    await expect(onMessage(msg("not an object"))).resolves.toBeUndefined()
    await expect(onMessage(msg({ nope: true }))).resolves.toBeUndefined()
    await expect(
      onMessage(msg({ ...(await envelope(50n)), event_version: 99 }))
    ).resolves.toBeUndefined()
    await expect(
      onMessage(msg({ ...(await envelope(50n)), event_type: "something else" }))
    ).resolves.toBeUndefined()

    expect(await balanceOf(assetId)).toBe(before)
  })

  it("applies an event once, even when delivered twice", async () => {
    const beforeAsset = await balanceOf(assetId)
    const beforeSystem = await balanceOf(systemId)

    const onMessage = makeMessageHandler(testTransaction, "balances-test")

    const env = await envelope(100n)

    await onMessage(msg(env))
    await onMessage(msg(env)) // redelivery

    expect((await balanceOf(assetId)) - beforeAsset).toBe(100n) // moved once
    expect((await balanceOf(systemId)) - beforeSystem).toBe(-100n)
  })

  it("lets a different consumer group apply the same event", async () => {
    const env = await envelope(100n)

    await makeMessageHandler(testTransaction, "group-a")(msg(env))
    await makeMessageHandler(testTransaction, "group-b")(msg(env))

    const { rows } = await client.query<{ consumer_group: string }>(
      `SELECT consumer_group FROM inbox WHERE event_id = $1 ORDER BY consumer_group`,
      [env.event_id]
    )

    expect(rows.map((r) => r.consumer_group)).toEqual(["group-a", "group-b"])
  })
})
