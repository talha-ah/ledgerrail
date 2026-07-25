import dotenv from "dotenv"
import * as path from "path"

dotenv.config({ path: path.resolve(process.cwd(), "../../.env") })

import { connectDB, getPool } from "./db.js"
import { runMigrations } from "./migrate.js"
import { loadConfig } from "./utils/loadConfig.js"

// Check required config - before anything else
loadConfig()

// const PORT = parseInt(process.env.PORT || "3000")
;(async () => {
  if (process.env.NODE_ENV === "test") return

  // Connect database
  await connectDB()

  // Run migrations
  await runMigrations(getPool())

  // Start server at ${PORT}
})()
