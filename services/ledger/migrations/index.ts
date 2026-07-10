import { type PoolClient } from "pg"
import { runner } from "node-pg-migrate"

export const migrations = async (client: PoolClient) => {
  try {
    console.log("Running migrations with active DB client...")

    // Pass the active client directly to the runner
    await runner({
      direction: "up",
      dbClient: client, // <--- Reuses your active connection here
      dir: "migrations",
      migrationsTable: "pgmigrations",
    })

    console.log("Migrations completed successfully.")
  } catch (error) {
    console.error("Migration failed:", error)
    process.exit(1)
  } finally {
    client.release() // Release the client back to the pool after migrations
  }
}
