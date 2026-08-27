import type { Pool, PoolClient } from "pg"
import { describe, it, beforeEach, afterEach, expect } from "vitest"

import { redrive } from "./redrive.js"
import { PermanentError } from "../utils/error.js"
import { randomString } from "../utils/helpers.js"
import type { DeadLetter } from "./deadLetters.js"
import { withDeadLetter } from "./withDeadLetter.js"
import { createAccounts } from "../../test/utils.js"
import { TOPICS, transactionKey } from "../kafka/index.js"
import { getTestPool, testTransaction } from "../../test/db.js"
import type { Event, TransactionPostedPayloadV1 } from "../types.js"
import { makeMessageHandler } from "../projections/balanceProjection/consumer.js"
import type {
  EventEnvelope,
  MessageHandler,
  IncomingMessage,
} from "../kafka/index.js"

describe("Dead letter queue", () => {
  const pool: Pool = getTestPool()

  let group: string
  let assetId: number
  let systemId: number
  let client: PoolClient

  // Offsets are the primary key, so every message in the file needs its own.
  let nextOffset = 0

  beforeEach(async () => {
    client = await pool.connect()
    const { s, a } = await createAccounts(client)
    assetId = a
    systemId = s
    group = `dlq-${randomString()}` // scope every assertion to this test's group
  })

  afterEach(() => client?.release())

  const envelope = async (amount: bigint): Promise<EventEnvelope> => {
    const payload: TransactionPostedPayloadV1 = {
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

  /** A message as it comes off the topic: bytes, undecoded. */
  const raw = (
    body: string | null,
    key: string | null = transactionKey("test")
  ): IncomingMessage => ({
    key,
    partition: 0,
    topic: TOPICS.LEDGER_EVENTS,
    offset: String(nextOffset++),
    raw: body === null ? null : Buffer.from(body, "utf-8"),
  })

  const encode = (value: unknown): string => JSON.stringify(value)

  const handler = () => makeMessageHandler(testTransaction, group)
  const wrapped = (inner: MessageHandler = handler()) =>
    withDeadLetter(inner, { pool, consumerGroup: group })

  async function deadLetters(): Promise<DeadLetter[]> {
    const { rows } = await pool.query<DeadLetter>(
      `SELECT * FROM dead_letters WHERE consumer_group = $1 ORDER BY kafka_offset`,
      [group]
    )
    return rows
  }

  async function balanceOf(accountId: number): Promise<bigint> {
    const { rows } = await client.query<{ amount: bigint }>(
      `SELECT amount FROM balances WHERE account_id = $1`,
      [accountId]
    )

    return rows[0]?.amount ?? 0n
  }

  async function cursorOf(accountId: number): Promise<string | null> {
    const { rows } = await client.query<{ last_event_seq: bigint }>(
      `SELECT last_event_seq FROM balances WHERE account_id = $1`,
      [accountId]
    )

    const seq = rows[0]?.last_event_seq
    return seq === undefined ? null : String(seq)
  }

  it("records an event_id that is not a UUID", async () => {
    // Why the column is TEXT. The id comes off an untrusted message, so it can be
    // any string at all — and a UUID column would reject the insert *inside* the
    // recovery path: no row, no log line, and the partition blocked forever on the
    // one message the DLQ exists to release.
    const poison = {
      event_id: "nope",
      event_type: "TransactionPosted",
      event_version: 99,
      payload: {},
    }

    await expect(wrapped()(raw(encode(poison)))).resolves.toBeUndefined()

    const [row] = await deadLetters()
    expect(row?.event_id).toBe("nope")
  })

  it("quarantines a message that is not valid JSON", async () => {
    const msg = raw("{ not json")

    await expect(wrapped()(msg)).resolves.toBeUndefined()

    const [row, ...rest] = await deadLetters()
    expect(rest).toHaveLength(0)
    expect(row?.error).toMatch(/invalid JSON/)
    expect(row?.event_id).toBeNull()
    // The bytes are kept exactly as they arrived — this is now the only copy.
    expect(row?.raw?.toString("utf-8")).toBe("{ not json")
    expect(String(row?.kafka_offset)).toBe(msg.offset)
  })

  it("quarantines a message with no key", async () => {
    await expect(
      wrapped()(raw(encode(await envelope(100n)), null))
    ).resolves.toBeUndefined()

    const [row] = await deadLetters()
    expect(row?.error).toMatch(/no key/)
    expect(row?.message_key).toBeNull()
  })

  it("quarantines a well-formed message the consumer cannot read", async () => {
    const env = { ...(await envelope(100n)), event_version: 99 }

    await expect(wrapped()(raw(encode(env)))).resolves.toBeUndefined()

    const [row] = await deadLetters()
    expect(row?.error).toMatch(/unknown event/)
    // Decoding got far enough to identify it, even though validation did not.
    expect(row?.event_id).toBe(env.event_id)
    expect(await balanceOf(assetId)).toBe(0n)
  })

  it("rethrows a transient failure instead of quarantining it", async () => {
    const outage = Object.assign(new Error("too many connections"), {
      code: "53300",
    })

    const inner: MessageHandler = async () => {
      throw outage
    }

    // Nothing recorded, so the offset never commits and the message comes back.
    await expect(
      wrapped(inner)(raw(encode(await envelope(100n))))
    ).rejects.toBe(outage)
    expect(await deadLetters()).toHaveLength(0)
  })

  it("does not block the message behind it", async () => {
    const onMessage = wrapped()

    await onMessage(raw("{ not json"))
    await onMessage(raw(encode(await envelope(100n))))

    expect(await balanceOf(assetId)).toBe(100n)
    expect(await deadLetters()).toHaveLength(1)
  })

  it("records one row when the same poison message is redelivered", async () => {
    const onMessage = wrapped()
    const msg = raw("{ not json")

    await onMessage(msg)
    await onMessage(msg) // same delivery position, same failure

    expect(await deadLetters()).toHaveLength(1)
  })

  it("redrives a quarantined event once the handler can read it", async () => {
    const env = await envelope(100n)

    // A handler that rejects everything — stands in for the bug that has since
    // been fixed. The message itself was always fine.
    const broken: MessageHandler = async () => {
      throw new PermanentError("unrecognized message: {}")
    }

    await wrapped(broken)(raw(encode(env)))
    expect(await balanceOf(assetId)).toBe(0n)

    const result = await redrive(pool, group, handler())

    expect(result).toEqual({ attempted: 1, resolved: 1, failed: 0 })
    expect(await balanceOf(assetId)).toBe(100n)
    expect(await balanceOf(systemId)).toBe(-100n)

    const [row] = await deadLetters()
    expect(row?.resolved_at).toBeInstanceOf(Date)
  })

  it("applies an event once even if it is redriven twice", async () => {
    const env = await envelope(100n)

    const broken: MessageHandler = async () => {
      throw new PermanentError("unrecognized message: {}")
    }

    await wrapped(broken)(raw(encode(env)))

    await redrive(pool, group, handler())

    // Nothing is unresolved now, so this second run has nothing to do — but the
    // inbox is what would stop a double-apply if it did.
    const second = await redrive(pool, group, handler())

    expect(second.attempted).toBe(0)
    expect(await balanceOf(assetId)).toBe(100n)
  })

  it("does not walk the projection cursor backwards on redrive", async () => {
    const older = await envelope(100n)
    const newer = await envelope(50n)

    const broken: MessageHandler = async () => {
      throw new PermanentError("unrecognized message: {}")
    }

    await wrapped(broken)(raw(encode(older))) // quarantined, offset moves on
    await wrapped()(raw(encode(newer))) // the one behind it applies

    expect(await cursorOf(assetId)).toBe(newer.global_seq)

    await redrive(pool, group, handler())

    // The amount takes both — adding deltas is commutative, so arriving out of
    // order costs nothing. The cursor is not: it must still name the furthest
    // event this projection has seen, not the last one it happened to apply.
    expect(await balanceOf(assetId)).toBe(150n)
    expect(await cursorOf(assetId)).toBe(newer.global_seq)
  })

  it("leaves a row unresolved when the redrive still fails", async () => {
    const env = { ...(await envelope(100n)), event_version: 99 }

    await wrapped()(raw(encode(env)))
    const result = await redrive(pool, group, handler())

    expect(result).toEqual({ attempted: 1, resolved: 0, failed: 1 })
    expect((await deadLetters())[0]?.resolved_at).toBeNull()
  })

  it("aborts the batch when a redrive hits a transient failure", async () => {
    const broken: MessageHandler = async () => {
      throw new PermanentError("unrecognized message: {}")
    }

    // Two quarantined messages, so there is a second row to leave untouched.
    await wrapped(broken)(raw(encode(await envelope(100n))))
    await wrapped(broken)(raw(encode(await envelope(50n))))

    const outage = Object.assign(new Error("too many connections"), {
      code: "53300",
    })

    const attempts: string[] = []
    const failing: MessageHandler = async (msg) => {
      attempts.push(msg.offset)
      throw outage
    }

    await expect(redrive(pool, group, failing)).rejects.toBe(outage)

    // The abort is the whole point: the second row was never attempted, and
    // neither was written off as permanently broken.
    expect(attempts).toHaveLength(1)

    const dlqs = await deadLetters()

    expect(dlqs).toHaveLength(2)
    expect(dlqs.every((r) => r.resolved_at === null)).toBe(true)
  })

  it("quarantines a version this build has never heard of", async () => {
    const env = { ...(await envelope(100n)), event_version: 99 }

    await expect(wrapped()(raw(encode(env)))).resolves.toBeUndefined()

    const [row] = await deadLetters()
    expect(row?.error).toMatch(/unknown event/)
    expect(await balanceOf(assetId)).toBe(0n)

    // The claim and the effect share a transaction, so a rejected event leaves
    // no inbox row behind — which is what lets a redrive apply it after the
    // consumer is upgraded, rather than skipping it as already handled.
    const { rows: inbox } = await pool.query(
      "SELECT event_id FROM inbox WHERE consumer_group = $1",
      [group]
    )

    expect(inbox).toHaveLength(0)
  })
})
