import { Pool } from "pg"
import type { TestProject } from "vitest/node"
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql"

import { seedReferenceData } from "./fixtures.js"
import { runMigrations } from "../src/migrate.js"

const IMAGE = "postgres:18"

/**
 * testcontainers reports every way of not finding a runtime through this one
 * message. Anything it doesn't match is rethrown untouched — a friendly message
 * wrapped around an unrelated failure is how that failure gets misdiagnosed.
 */
function isMissingContainerRuntime(err: unknown): err is Error {
  return (
    err instanceof Error &&
    /container runtime|docker daemon|ECONNREFUSED/i.test(err.message)
  )
}

function explain(err: Error): string {
  return [
    "",
    "  ┌─ Cannot start the test database ─────────────────────────────",
    "  │",
    "  │  These tests run against a real Postgres in a throwaway",
    "  │  container, and no container runtime is running.",
    "  │",
    "  │    1. Start Docker Desktop (or `colima start`)",
    "  │    2. Wait for it to report ready",
    "  │    3. pnpm test",
    "  │",
    "  │  Note: the Postgres in infra/docker-compose.yml is a different",
    "  │  database. Tests always spin up their own and drop it after.",
    "  │",
    `  │  Underlying error: ${err.message}`,
    "  └──────────────────────────────────────────────────────────────",
    "",
  ].join("\n")
}

export default async function (project: TestProject) {
  let container: StartedPostgreSqlContainer

  try {
    console.log(`[Test DB] starting ${IMAGE} - the first run pulls the image`)
    container = await new PostgreSqlContainer(IMAGE).start()
  } catch (err) {
    if (!isMissingContainerRuntime(err)) throw err
    console.error(explain(err))
    // Cause is kept deliberately: the pretty block is for you, the cause is for
    // the day this matches something that isn't actually a missing runtime.
    throw new Error("no container runtime available", { cause: err })
  }

  const uri = container.getConnectionUri()
  const pool = new Pool({ connectionString: uri })
  pool.on("error", (err) =>
    console.error("[Test DB] idle client error:", err.message)
  )

  try {
    await runMigrations(pool)
    await seedReferenceData(pool)
  } catch (error) {
    // The pool goes first. A pool whose server has vanished emits an unhandled
    // 'error' from its idle clients, which crashes the process — and takes the
    // real error with it, which is how a seed failure becomes "0 tests, no
    // explanation".
    await pool.end().catch(() => {})
    await container.stop()
    throw error
  }

  project.provide("pgUri", uri)

  await pool.end()

  return async () => await container.stop()
}

declare module "vitest" {
  export interface ProvidedContext {
    pgUri: string
  }
}
