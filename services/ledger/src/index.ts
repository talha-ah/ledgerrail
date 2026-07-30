import dotenv from "dotenv"
import * as path from "path"

dotenv.config({ path: path.resolve(process.cwd(), "../../.env") })

import { connectDB, getPool } from "./db.js"
import { runMigrations } from "./migrate.js"
import { loadConfig } from "./utils/loadConfig.js"

export async function bootstrap() {
  // Check required config - before anything else
  loadConfig()

  // Connect database
  await connectDB()

  // Run migrations
  await runMigrations(getPool())
}

// // const PORT = parseInt(process.env.PORT || "3000")
// ;(async () => {
//   if (process.env.NODE_ENV === "test") return

//   await bootstrap()
//   // Start server at ${PORT}
// })()
