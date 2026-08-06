import { getKafka } from "./client.js"

export interface IncomingMessage {
  topic: string
  partition: number
  offset: string
  key: string | null
  value: unknown
}

export type MessageHandler = (msg: IncomingMessage) => Promise<void>

export interface ConsumerHandle {
  disconnect(): Promise<void>
}

export interface Opts {
  topic: string
  groupId: string
  fromBeginning?: boolean
  onMessage: MessageHandler
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
      // We'll add validation gate later

      if (!message.key) {
        console.error(
          `[CONSUMER] Empty key detected for: ${topic}:${partition}:${describeValue(message)}`
        )
        return
      }

      await opts.onMessage({
        topic,
        partition,
        offset: message.offset,
        key: String(message.key || "") ?? null,
        value: message.value ? JSON.parse(message.value.toString()) : null,
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
