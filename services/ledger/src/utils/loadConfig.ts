const required = [
  "POSTGRES_PORT",
  "POSTGRES_HOST",
  "POSTGRES_DB",
  "POSTGRES_USER",
  "POSTGRES_PASSWORD",
]

export function loadConfig() {
  const ENV = process.env

  const notFound: string[] = []

  required.forEach((env) => !ENV[env] && notFound.push(env))

  if (notFound.length > 0) {
    throw new Error(`Missing required ENVs ${notFound.join(", ")}`)
  }

  return true
}
