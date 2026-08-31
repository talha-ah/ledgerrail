import { Kafka } from "kafkajs"

let kafka: Kafka | null = null

export function getKafka(): Kafka {
  if (kafka) return kafka

  // A list, not a broker. These are seeds used only for bootstrap — the client
  // learns the real topology from whichever one answers. One seed means killing
  // that broker breaks the client for reasons unrelated to the cluster's health,
  // which is how a connection error gets mistaken for a quorum failure.
  const brokers = (process.env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean)

  if (brokers.length === 0) throw new Error("KAFKA_BROKER is not set")

  kafka = new Kafka({ clientId: "ledger", brokers })

  return kafka
}

/** Drops the memoized client, and with it kafkajs's cached view of the cluster. */
export function resetKafka() {
  kafka = null
}
