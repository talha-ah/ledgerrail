export type * from "./types.js"
export { getKafka } from "./client.js"
export { createTopic, createTopics } from "./admin.js"
export { createProducer, type ProducerHandle } from "./producer.js"
export {
  TOPICS,
  GROUPS,
  accountKey,
  transactionKey,
  DEFAULT_PARTITIONS,
  DEFAULT_REPLICATION,
} from "./utils.js"
export {
  describeValue,
  createConsumer,
  type MessageHandler,
  type ConsumerHandle,
  type IncomingMessage,
} from "./consumer.js"
