import type { Pool } from "pg"

import { recordDeadLetter } from "./deadLetters.js"
import { PermanentError, isPermanent, errorMessage } from "../utils/error.js"
import type {
  DecodedMessage,
  MessageHandler,
  IncomingMessage,
  RawMessageHandler,
} from "../kafka/index.js"

export function decode(raw: Buffer | null): unknown {
  // A null value is a tombstone. On a compacted topic that means something; on
  // an event log it means a producer bug, and no retry will fill it in.
  if (raw === null) throw new PermanentError("message has no value")

  try {
    return JSON.parse(raw.toString("utf8"))
  } catch (err) {
    throw new PermanentError(`invalid JSON: ${errorMessage(err)}`, {
      cause: err,
    })
  }
}

export interface DeadLetterOpts {
  pool: Pool
  consumerGroup: string
}

/**
 * Turns a handler that processes decoded messages into one the consumer can run
 * against raw bytes, quarantining anything that can never succeed.
 *
 * Lives here rather than inside the projection so the projection stays about
 * projecting, and so the next consumer group inherits the behaviour instead of
 * reimplementing it.
 */
export function withDeadLetter(
  inner: MessageHandler,
  { pool, consumerGroup }: DeadLetterOpts
): RawMessageHandler {
  return async (msg: IncomingMessage) => {
    let decoded: DecodedMessage | null = null

    try {
      // Without a key the broker chose a partition at random, so this message has
      // no ordering relative to the others for its account. It is a broken
      // producer, not a slow one.
      if (!msg.key) throw new PermanentError("message has no key")

      decoded = { ...msg, value: decode(msg.raw) }
      await inner(decoded)
    } catch (err) {
      // Transient: rethrow. The offset stays uncommitted, kafkajs redelivers, and
      // partition blocks until the dependency recovers. That is deliberate -
      // a retry budget would empty the whole topic into this table during a
      // thirty-second database outage.
      if (!isPermanent(err)) throw err

      // Deliberately on the pool, not on the handler's client: that transaction
      // has already rolled back. And deliberately not caught - if we cannot
      // record the message, the offset must not advance past it.
      await recordDeadLetter(pool, consumerGroup, decoded ?? msg, err)

      console.error(
        `[DLQ] quarantined ${msg.topic}/${msg.partition}@${msg.offset}(${msg.key}): ${errorMessage(err)}`
      )
    }
  }
}
