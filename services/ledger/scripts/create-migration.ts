import * as fs from "fs"
import * as path from "path"
import { fileURLToPath } from "url"

const MIGRATIONS_DIR = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "migrations"
)

async function main() {
  // 1. Get the migration name from command line arguments
  const args = process.argv.slice(2)
  const migrationName = args[0]

  if (!migrationName) {
    console.error("❌ Error: Please provide a migration name.")
    console.error("👉 Example: npm run migration:create add_users_table")
    process.exit(1)
  }

  // 2. Format name to safely remove spaces or weird characters
  const sanitizedName = migrationName
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_") // Replace non-alphanumeric with underscores
    .replace(/^_+|_+$/g, "") // Trim leading/trailing underscores

  // 3. Generate high-resolution YYYYMMDDHHMMSS UTC timestamp
  const now = new Date()
  const timestamp = now
    .toISOString()
    .replace(/[-T:]/g, "") // Remove date separators
    .split(".")[0] // Strip milliseconds

  const filename = `${timestamp}_${sanitizedName}.sql`
  const filePath = path.join(MIGRATIONS_DIR, filename)

  try {
    // 4. Ensure migrations folder exists and write the file
    await fs.mkdirSync(MIGRATIONS_DIR, { recursive: true })

    // Add a helpful comment at the top of the file
    const initialContent = `-- Migration: ${sanitizedName}\n-- Created at: ${now.toUTCString()}\n\n-- Write your forward-only DDL here\n`

    await fs.writeFileSync(filePath, initialContent, "utf-8")

    console.log(`\n✅ Migration created successfully!`)
    console.log(`📂 File: migrations/${filename}\n`)
  } catch (error) {
    console.error("❌ Failed to create migration file:", error)
    process.exit(1)
  }
}

main()
