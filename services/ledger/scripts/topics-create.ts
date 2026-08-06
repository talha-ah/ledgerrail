import { loadEnv } from "../src/utils/env.js"
import { createTopics } from "../src/kafka/index.js"

export async function main() {
  loadEnv()
  await createTopics()
}

main()
