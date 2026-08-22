import { getKafka } from "./client.js"
import {
  TOPICS,
  DEFAULT_PARTITIONS,
  DEFAULT_REPLICATION,
  MIN_INSYNC_REPLICAS,
} from "./index.js"

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
            configEntries: [
              {
                name: "min.insync.replicas",
                value: String(MIN_INSYNC_REPLICAS),
              },
            ],
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

    if (newTopics.length <= 0) {
      console.log(
        `[TOPICS] nothing to do — ${topics.length} topic(s) already exist`
      )
      return
    }

    const input = newTopics.map((t) => ({
      topic: t,
      numPartitions: DEFAULT_PARTITIONS,
      replicationFactor: DEFAULT_REPLICATION,
      configEntries: [
        {
          name: "min.insync.replicas",
          value: String(MIN_INSYNC_REPLICAS),
        },
      ],
    }))

    await admin.createTopics({ topics: input })

    console.log(
      `[TOPICS] created ${input
        .map(
          (t) => `${t.topic} (${t.numPartitions}p, rf=${t.replicationFactor})`
        )
        .join(", ")}`
    )
  } finally {
    await admin.disconnect()
  }
}
