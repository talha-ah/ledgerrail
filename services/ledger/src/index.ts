import { loadEnv } from "./utils/env.js"
import { runMigrations } from "./migrate.js"
import { getPool, connectDB, disconnectDB } from "./db.js"

export async function bootstrap() {
  loadEnv()
  await connectDB()
  await runMigrations(getPool())
}

export async function shutdown() {
  await disconnectDB()
}
