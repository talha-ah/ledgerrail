import { Pool } from "pg"
import type { TestProject } from "vitest/node"
import { PostgreSqlContainer } from "@testcontainers/postgresql"

import { seedReferenceData } from "./fixtures.js"
import { runMigrations } from "../src/migrate.js"

export default async function (project: TestProject) {
  const container = await new PostgreSqlContainer("postgres:18").start()
  const uri = container.getConnectionUri()

  const pool = new Pool({ connectionString: uri })

  try {
    await runMigrations(pool)
    await seedReferenceData(pool)
  } catch (error) {
    await container.stop()
    throw error
  } finally {
    await pool.end()
  }

  project.provide("pgUri", uri)

  return async () => await container.stop()
}

declare module "vitest" {
  export interface ProvidedContext {
    pgUri: string
  }
}
