import { getKafka } from "./client.js"
import { TOPICS, DEFAULT_PARTITIONS, DEFAULT_REPLICATION } from "./index.js"

export async function createTopic(
  topic: string,
  partitions: number = DEFAULT_PARTITIONS,
  replicas: number = DEFAULT_REPLICATION
) {
  const admin = getKafka().admin()

  try {
    await admin.connect()

    const topics = await admin.listTopics()

    if (!topics.includes(topic)) {
      await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions: partitions,
            replicationFactor: replicas,
          },
        ],
      })
    }
  } finally {
    await admin.disconnect()
  }
}

export async function createTopics() {
  const admin = getKafka().admin()

  try {
    await admin.connect()

    const topics = await admin.listTopics()

    const newTopics = []
    for (const topic of Object.values(TOPICS)) {
      if (!topics.includes(topic)) newTopics.push(topic)
    }

    if (newTopics.length <= 0) return

    const input = newTopics.map((t) => ({
      topic: t,
      numPartitions: DEFAULT_PARTITIONS,
      replicationFactor: DEFAULT_REPLICATION,
    }))

    await admin.createTopics({ topics: input })
  } finally {
    await admin.disconnect()
  }
}
