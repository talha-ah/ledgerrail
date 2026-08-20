import type { Pool } from "pg"

import { errorCode, errorMessage } from "../utils/error.js"
import type { IncomingMessage, DecodedMessage } from "../kafka/index.js"

function eventIdOf(value: unknown): string | null {
  if (typeof value !== "object" || value === null) return null

  const id = (value as { event_id?: unknown }).event_id
  return typeof id === "string" ? id : null
}

export interface DeadLetter {
  consumer_group: string
  topic: string
  partition: number
  kafka_offset: bigint
  event_id: string | null
  message_key: string | null
  raw: Buffer | null
  error: string
  error_code: string | null
  failed_at: Date
  resolved_at: Date | null
}

export async function recordDeadLetter(
  pool: Pool,
  consumerGroup: string,
  msg: IncomingMessage | DecodedMessage,
  err: unknown
): Promise<void> {
  await pool.query(
    `
    INSERT INTO dead_letters
    (consumer_group, topic, partition, kafka_offset, event_id, message_key, raw, error, error_code)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (consumer_group, topic, partition, kafka_offset) DO UPDATE
      SET error = EXCLUDED.error,
        error_code = EXCLUDED.error_code, 
        failed_at = NOW(), 
        resolved_at = NULL
      -- Reopen only a row somebody already closed: a redrive that appeared to
      -- succeed and then failed again is news. An unresolved row keeps its
      -- first-seen failure, because a redelivery of a poison message is the
      -- same failure arriving twice, not a new one.
    WHERE dead_letters.resolved_at IS NOT NULL; 
  `,
    [
      consumerGroup,
      msg.topic,
      msg.partition,
      msg.offset,
      "value" in msg ? eventIdOf(msg.value) : null,
      msg.key,
      msg.raw,
      errorMessage(err),
      errorCode(err) ?? null,
    ]
  )
}

export async function listUnresolved(
  pool: Pool,
  consumerGroup: string,
  limit = 50
): Promise<DeadLetter[]> {
  const { rows } = await pool.query<DeadLetter>(
    `
    SELECT consumer_group, topic, partition, kafka_offset, event_id, message_key, raw, error, error_code, failed_at, resolved_at
    FROM dead_letters
    WHERE consumer_group = $1 AND resolved_at IS NULL
    ORDER BY failed_at, partition, kafka_offset
    LIMIT $2
    `,
    [consumerGroup, limit]
  )

  return rows
}

export async function markResolved(
  pool: Pool,
  row: Pick<
    DeadLetter,
    "consumer_group" | "topic" | "partition" | "kafka_offset"
  >
): Promise<void> {
  // Resolved, not deleted. How often a message dead-letter, and how long it sat
  // there, is the operational signal - and unlike events or projections, this row
  // cannot be reconstructed from anything else once it is gone.
  await pool.query(
    `
    UPDATE dead_letters
    SET resolved_at = NOW()
    WHERE consumer_group = $1 AND topic = $2 AND partition = $3 AND kafka_offset = $4;
    `,
    [row.consumer_group, row.topic, row.partition, String(row.kafka_offset)]
  )
}
