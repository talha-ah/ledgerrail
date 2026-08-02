import { getKafka } from "./client.js"

export interface ProducerHandle {
  send(topic: string, key: string, value: unknown): Promise<void>
  disconnect(): Promise<void>
}

export async function createProducer(): Promise<ProducerHandle> {
  const producer = getKafka().producer()
  await producer.connect()

  return {
    async send(topic, key, value) {
      await producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(value) }],
      })
    },
    disconnect: () => producer.disconnect(),
  }
}
