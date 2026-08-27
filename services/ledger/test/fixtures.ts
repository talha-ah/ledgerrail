import { type Pool } from "pg"

export const SYSTEM_ACCOUNT = {
  type: "system",
  accountNumber: "ABC123233",
  currency: "PKR",
}

export const ASSET_ACCOUNT = {
  type: "asset",
  accountNumber: "DEF132323",
  currency: "PKR",
}

const ACCOUNTS = [SYSTEM_ACCOUNT, ASSET_ACCOUNT]

export async function seedReferenceData(pool: Pool) {
  const client = await pool.connect()

  try {
    for (const account of ACCOUNTS) {
      await client.query(
        `INSERT INTO accounts (type, account_number, currency)
      VALUES ($1, $2, $3)`,
        [account.type, account.accountNumber, account.currency]
      )
    }

    // Scoped to what this seed inserted. Counting every row in the table made
    // the fixture assume it was the only thing that ever creates an account —
    // which stopped being true the moment a migration seeded reserve accounts.
    const { rows } = await client.query<{ n: number }>(
      "SELECT COUNT(*)::int AS n FROM accounts WHERE account_number = ANY($1)",
      [ACCOUNTS.map((a) => a.accountNumber)]
    )

    if (rows[0]?.n !== ACCOUNTS.length) throw new Error("Accounts seed failed")
  } finally {
    client.release()
  }
}
