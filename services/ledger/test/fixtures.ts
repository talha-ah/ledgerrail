import { type Pool } from "pg"
import { expect } from "vitest"

const ACCOUNTS = [
  {
    id: 1,
    accountNumber: "ABC123233",
    currency: "PKR",
  },
  {
    id: 2,
    accountNumber: "DEF132323",
    currency: "PKR",
  },
]

export async function seedReferenceData(pool: Pool) {
  const client = await pool.connect()

  for (const account of ACCOUNTS) {
    await client.query(
      `INSERT INTO accounts (id, account_number, currency)
      VALUES ($1, $2, $3)`,
      [account.id, account.accountNumber, account.currency]
    )
  }

  const { rows: accounts } = await client.query("SELECT * FROM accounts")
  expect(accounts.length).toBe(2)

  client.release()
}
