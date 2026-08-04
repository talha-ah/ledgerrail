import type { PoolClient } from "pg"
import { describe, it, beforeAll, afterAll, expect } from "vitest"

import type { Event } from "../types.js"
import { publishBatch } from "./relay.js"
import { getTestPool } from "../../test/db.js"
import type { EventEnvelope } from "../kafka/types.js"
import type { ProducerHandle } from "../kafka/producer.js"
import { createAccounts, postTransactions } from "../../test/utils.js"

async function getOutboxCount(client: PoolClient) {
  const { rows: outbox } = await client.query<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM outbox WHERE published_at is NULL;`
  )

  return outbox[0]?.n ?? 0
}

// This will be flaky - when happens - only assert events created in this test
describe("Relay", () => {
  const pool = getTestPool()

  let client: PoolClient

  beforeAll(async () => {
    client = await pool.connect()

    const { s, a } = await createAccounts(client)

    await postTransactions(s, a, client)
  })

  afterAll(async () => {
    client?.release()
    await pool?.end()
  })

  it("Happy path", async () => {
    const sent: Array<{ topic: string; key: string; value: EventEnvelope }> = []

    const fakeProducer: ProducerHandle = {
      send: async (topic, key, value) => {
        sent.push({ topic, key, value })
      },
      disconnect: async () => {},
    }

    const { rows: events } = await client.query<Pick<Event, "event_id">>(
      "SELECT event_id FROM events ORDER BY global_seq;"
    )

    const beforeCount = await getOutboxCount(client)
    expect(beforeCount).toBeGreaterThan(0)

    await publishBatch(pool, fakeProducer, beforeCount)

    const afterCount = await getOutboxCount(client)
    expect(afterCount).toBe(0)

    expect(sent).toHaveLength(beforeCount)
    expect(sent.map((s) => s.value.event_id)).toEqual(
      events.map((e) => e.event_id)
    )
    expect(sent[0]?.key).toBe(`transaction:${sent[0]?.value.event_id}`)
    expect(sent[0]?.value.event_type).toBe("TransactionPosted")
    expect(sent[0]?.value.event_version).toBe(1)
    expect(sent[0]?.value.payload.postings).toHaveLength(2)
  })
})
