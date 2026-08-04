import { getKafka } from "./client.js"
import type { EventEnvelope } from "./types.js"

export interface ProducerHandle {
  send(topic: string, key: string, value: EventEnvelope): Promise<void>
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
    disconnect: async () => {
      await producer.disconnect()
      console.log("[PRODUCER] Disconnected")
    },
  }
}
