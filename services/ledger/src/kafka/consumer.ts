import { getKafka } from "./client.js"

export interface IncomingMessage {
  topic: string
  partition: number
  offset: string
  key: string | null
  raw: Buffer | null
}

export interface DecodedMessage extends IncomingMessage {
  value: unknown
}

export type RawMessageHandler = (msg: IncomingMessage) => Promise<void>
export type MessageHandler = (msg: DecodedMessage) => Promise<void>

export interface ConsumerHandle {
  disconnect(): Promise<void>
}

export interface Opts {
  topic: string
  groupId: string
  fromBeginning?: boolean
  onMessage: RawMessageHandler
}

export function describeValue(v: unknown): string {
  try {
    const s = JSON.stringify(v, (_k, val) =>
      typeof val === "bigint" ? val.toString() : val
    )
    return s === undefined ? String(v) : s.slice(0, 500)
  } catch {
    return `<unserializable ${typeof v}>`
  }
}

export async function createConsumer(opts: Opts): Promise<ConsumerHandle> {
  const consumer = getKafka().consumer({ groupId: opts.groupId })
  await consumer.connect()
  await consumer.subscribe({
    topic: opts.topic,
    fromBeginning: opts.fromBeginning ?? false,
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      // No validation, no filtering, no early return: every message reaches the
      // handler. Dropping one here would commit its offset and lose it silently,
      // which is the one outcome worse than blocking.

      await opts.onMessage({
        topic,
        partition,
        offset: message.offset,
        key: message.key ? String(message.key) : null,
        raw: message.value,
      })
    },
  })

  return {
    disconnect: async () => {
      await consumer.disconnect()
      console.log("[CONSUMER] Disconnected ", opts.groupId)
    },
  }
}
