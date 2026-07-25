import { Pool, type PoolClient, type QueryResult } from "pg"

let pool: Pool | null = null

export const getPool = (): Pool => {
  if (!pool) {
    pool = new Pool({
      host: process.env.POSTGRES_HOST,
      user: process.env.POSTGRES_USER,
      database: process.env.POSTGRES_DB,
      password: process.env.POSTGRES_PASSWORD,
      port: parseInt(process.env.POSTGRES_PORT || "5432", 10),
      max: 10, // Maximum active connections
      idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
    })
  }

  return pool
}

export const connectDB = async (): Promise<void> => {
  try {
    const localPool = getPool() // Initialize the pool

    await localPool.connect()
    console.log("Connected to the database successfully.")
  } catch (err) {
    console.error("Error connecting to the database:", err)
    throw err
  }
}

export const query = async <T extends object>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> => {
  const pool = getPool() // Ensure the pool is initialized before querying

  // pool.query handles checking out and releasing connections automatically
  return pool.query<T>(text, params)
}

export const transaction = async <T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> => {
  const pool = getPool() // Ensure the pool is initialized before starting a transaction
  const client = await pool.connect()

  try {
    await client.query("BEGIN")
    const result = await callback(client)
    await client.query("COMMIT")
    return result
  } catch (err) {
    await client.query("ROLLBACK")
    throw err
  } finally {
    client.release()
  }
}
