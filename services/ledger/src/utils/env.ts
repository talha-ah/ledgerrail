import dotenv from "dotenv"
import * as path from "path"
import { fileURLToPath } from "url"

const required = [
  "POSTGRES_PORT",
  "POSTGRES_HOST",
  "POSTGRES_DB",
  "POSTGRES_USER",
  "POSTGRES_PASSWORD",
  "KAFKA_BROKER",
]

function verifyEnvs() {
  const ENV = process.env

  const notFound: string[] = []

  required.forEach((env) => !ENV[env] && notFound.push(env))

  if (notFound.length > 0) {
    throw new Error(`Missing required ENVs ${notFound.join(", ")}`)
  }

  return true
}

let loaded = false
export function loadEnv(): void {
  if (loaded) return

  const here = path.dirname(fileURLToPath(import.meta.url))
  dotenv.config({ path: path.resolve(here, "../../../../.env") })
  loaded = true

  verifyEnvs()
}
