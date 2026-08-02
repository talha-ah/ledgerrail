import { getKafka } from "./client.js"

export interface IncomingMessage {
  topic: string
  partition: number
  offset: string
  key: string
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

export async function createConsumer(opts: Opts): Promise<ConsumerHandle> {
  const consumer = getKafka().consumer({ groupId: opts.groupId })
  await consumer.connect()
  await consumer.subscribe({
    topic: opts.topic,
    fromBeginning: opts.fromBeginning ?? false,
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await opts.onMessage({
        topic,
        partition,
        offset: message.offset,
        key: String(message.key || "") ?? null,
        value: message.value ? JSON.parse(message.value.toString()) : null,
      })
    },
  })

  return { disconnect: () => consumer.disconnect() }
}
