import { Kafka } from "kafkajs"

let kafka: Kafka | null = null

export function getKafka(): Kafka {
  if (kafka) return kafka

  const broker = process.env.KAFKA_BROKER
  if (!broker) throw new Error("KAFKA_BROKER is not set")

  kafka = new Kafka({ clientId: "ledger", brokers: [broker] })

  return kafka
}
