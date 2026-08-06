import { loadEnv } from "../src/utils/env.js"
import {
  createTopic,
  createProducer,
  createConsumer,
  type IncomingMessage,
} from "../src/kafka/index.js"

const TOPIC = "some-topic"

function deferred<T>() {
  let resolve: (v: T) => void = () => {}
  const promise = new Promise((r) => {
    resolve = r
  })
  return { promise, resolve }
}

async function main() {
  loadEnv()
  await createTopic(TOPIC)

  const received = deferred<IncomingMessage>()

  const consumer = await createConsumer({
    topic: TOPIC,
    groupId: "exp-1",
    onMessage: async (msg: IncomingMessage) => {
      console.log(`[${msg.topic}/${msg.partition}@${msg.offset}]`, msg.value)
      received.resolve(msg)
    },
  })

  const producer = await createProducer()
  await producer.send(TOPIC, "demo-key", {
    event_id: "did",
    global_seq: String(1n),
    event_type: "TransactionPosted",
    created_at: new Date().toISOString(),
    event_version: 1,
    payload: {
      idempotencyKey: "asdfasd",
      currency: "PKR",
      postings: [
        { accountId: 1, amount: "10" },
        { accountId: 2, amount: "-10" },
      ],
    },
  })

  await Promise.race([
    received.promise,
    new Promise((_, rejects) =>
      setTimeout(() => rejects(new Error("no message withing 10s")), 10_000)
    ),
  ])

  await producer.disconnect()
  await consumer.disconnect()
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
