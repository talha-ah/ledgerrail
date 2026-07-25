import * as fs from "fs"
import * as path from "path"
import { fileURLToPath } from "url"

import { type Pool } from "pg"

const MIGRATION_LOCK_ID = 8472910382910n
const MIGRATIONS_DIR = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "migrations"
)

/**
 * Migrations Runner
 *
 * Essential Architectural Rules to Follow
 * - Never modify an applied file: Once a migration file (e.g., 0001_init.sql) is deployed to production, it is immutable. Treat it like a permanent ledger entry.
 * - Fix forward: If you need to add a column or alter a data type later, create 0002_add_billing.sql. Do not edit 0001_init.sql retroactively.
 * - Keep DDL transaction-safe: Avoid commands like CREATE INDEX CONCURRENTLY or VACUUM inside these migration files, as Postgres forbids running them inside multi-command transactions.
 *
 * @param pool
 * @returns Promise<void>
 */

export async function runMigrations(pool: Pool): Promise<void> {
  console.log("[Migrations] 🔄 Starting migration runner...")

  const client = await pool.connect()

  try {
    // 1. Acquire global advisory lock to prevent race conditions during boot
    // This blocks other instances until this process finishes or crashes
    await client.query("SELECT pg_advisory_lock($1);", [MIGRATION_LOCK_ID])
    console.log("[Migrations] 🔒 Acquired exclusive migration advisory lock.")

    // 2. Ensure schema_migrations tracking table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ DEFAULT NOW()
      )
    `)

    // 3. Read and sort local migration files lexicographically
    if (!fs.existsSync(MIGRATIONS_DIR)) {
      fs.mkdirSync(MIGRATIONS_DIR, { recursive: true })
    }

    const allFiles = fs.readdirSync(MIGRATIONS_DIR)
    const sqlFiles = allFiles.filter((file) => file.endsWith(".sql")).sort() // Properly sorts zero-padded prefixes (0001_, 0002_)

    if (sqlFiles.length === 0) {
      console.log("[Migrations] ✅ No migration files found in directory.")
      return
    }

    // 4. Fetch already applied versions from the database
    const { rows } = await client.query(
      "SELECT version FROM schema_migrations;"
    )
    const appliedVersions = new Set(rows.map((row) => row.version))

    // 5. Filter out pending migrations
    const pendingFiles = sqlFiles.filter((file) => !appliedVersions.has(file))

    if (pendingFiles.length === 0) {
      console.log(
        "[Migrations] ✅ Database is up to date. Zero migrations to apply."
      )
      return
    }

    console.log(
      `[Migrations] 📂 Found ${pendingFiles.length} pending migration(s) to execute.`
    )

    for (const file of pendingFiles) {
      const filePath = path.join(MIGRATIONS_DIR, file)
      const sqlContent = fs.readFileSync(filePath, "utf-8")

      console.log(`[Migrations] 🚀 Executing migration: ${file}`)

      // Leverage Postgres Transactional DDL
      await client.query("BEGIN;")
      try {
        // Run migration schema changes
        await client.query(sqlContent)

        // Record successful application
        await client.query(
          `INSERT INTO schema_migrations (version) VALUES ($1);`,
          [file]
        )

        await client.query("COMMIT;")
        console.log(
          `[Migrations] ✨ Successfully applied and committed: ${file}`
        )
      } catch (error) {
        // If anything fails inside the file, the entire DDL batch rolls back cleanly
        await client.query("ROLLBACK;")
        console.error(
          `[Migrations] ❌ Migration failed and rolled back cleanly: ${file}`
        )
        throw error // Halt the boot process completely
      }
    }

    console.log("[Migrations] 🎉 All migrations processed successfully.")
  } finally {
    // 7. Always release the advisory lock, even if migrations fail
    await client.query(`SELECT pg_advisory_unlock($1);`, [MIGRATION_LOCK_ID])
    console.log("[Migrations] 🔓 Released migration advisory lock.")

    client.release()
  }
}
