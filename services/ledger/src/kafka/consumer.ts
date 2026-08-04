import { getKafka } from "./client.js"
import type { EventEnvelope } from "./types.js"

export interface IncomingMessage {
  topic: string
  partition: number
  offset: string
  key: string | null
  value: EventEnvelope
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
        console.log(
          `[CONSUMER] Empty key detected for: ${topic}:${partition}:${JSON.stringify(message)}`
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
