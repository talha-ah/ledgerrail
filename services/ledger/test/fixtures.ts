import { type Pool } from "pg"

export const SYSTEM_ACCOUNT = {
  id: 1,
  type: "system",
  accountNumber: "ABC123233",
  currency: "PKR",
}

export const ASSET_ACCOUNT = {
  id: 2,
  type: "asset",
  accountNumber: "DEF132323",
  currency: "PKR",
}

const ACCOUNTS = [SYSTEM_ACCOUNT, ASSET_ACCOUNT]

export async function seedReferenceData(pool: Pool) {
  const client = await pool.connect()

  for (const account of ACCOUNTS) {
    await client.query(
      `INSERT INTO accounts (id, type, account_number, currency)
      VALUES ($1, $2, $3, $4)`,
      [account.id, account.type, account.accountNumber, account.currency]
    )
  }

  const { rows: accounts } = await client.query("SELECT * FROM accounts")
  if (accounts.length !== 2) throw new Error("Accounts seed failed")

  // Inserting explicit ids doesn't advance the sequence.
  // so manually setting the sequence to max id. PG Failure (This is a real production incident pattern too: restore a database dump that carries explicit ids, forget to reset the sequences, and your app starts throwing duplicate-key errors on the very next insert.)
  await client.query(
    `SELECT setval(pg_get_serial_sequence('accounts','id'), (SELECT MAX(id) FROM accounts));`
  )

  client.release()
}
