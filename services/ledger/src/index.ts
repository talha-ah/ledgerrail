import dotenv from "dotenv"
import * as path from "path"

dotenv.config({ path: path.resolve(process.cwd(), "../../.env") })

import { runMigrations } from "./migrate.js"
import { loadConfig } from "./utils/loadConfig.js"
import { getPool, connectDB, disconnectDB } from "./db.js"

export async function bootstrap() {
  loadConfig()
  await connectDB()
  await runMigrations(getPool())
}

export async function shutdown() {
  await disconnectDB()
}
