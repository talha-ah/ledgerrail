import { getKafka } from "./client.js"

export async function createTopic(
  topic: string,
  partitions: number = 1,
  replicas: number = 1
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
