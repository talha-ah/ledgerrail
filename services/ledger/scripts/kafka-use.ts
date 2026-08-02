import { bootstrap } from "../src/index.js"
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
  await bootstrap()
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
  await producer.send(TOPIC, "demo-key", "Hi How are you?")

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
