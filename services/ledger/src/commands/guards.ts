import type { PoolClient } from "pg"

import type { Currency } from "@ledgerrail/money"
import type { Account, AccountBalances, Posting } from "../types.js"

export interface Leg {
  accountId: number
  amount: bigint
  currency: Currency
}

export function toPostings(legs: Leg[]): Posting[] {
  return legs.map((l) => ({
    accountId: l.accountId,
    amount: String(l.amount),
    currency: l.currency,
  }))
}

/**
 * Everything that must be true before money moves, in the order it must be true.
 * Legs are sorted by account id so concurrent commands take row locks in the same
 * order and cannot deadlock.
 */
export async function assertPostable(
  legs: Leg[],
  client: PoolClient
): Promise<Leg[]> {
  const sorted = [...legs].sort((a, b) => a.accountId - b.accountId)

  if (sorted.length < 2) throw new Error("Less legs founds")
  if (sorted.some((l) => l.amount === 0n)) throw new Error("Invalid legs found")
  if (sorted.reduce((s, l) => s + l.amount, 0n) !== 0n)
    throw new Error("Invalid transaction amounts")

  const { rows: accounts } = await client.query<Account>(
    "Select * FROM accounts WHERE id = ANY($1)",
    [sorted.map((l) => l.accountId)]
  )

  const byId = new Map(accounts.map((a) => [a.id, a]))

  for (const leg of sorted) {
    const account = byId.get(leg.accountId)
    if (!account) throw new Error("Invalid accounts")
    if (account.currency !== leg.currency) throw new Error("Invalid currency")
  }

  for (const leg of sorted) {
    if (leg.amount > 0n) continue // only debit can overdraw

    const account = byId.get(leg.accountId)
    if (!account) throw new Error("Invalid accounts")

    // The type check precedes the lock deliberately. System accounts have no
    // floor, and locking one would serialise every funding transaction — and now
    // every hold — on a single row (ADR 0002).
    if (account.type === "system") continue

    const { rows: balances } = await client.query<
      Pick<AccountBalances, "pending" | "posted">
    >(
      "SELECT pending, posted FROM account_balances WHERE account_id = $1 FOR UPDATE",
      [leg.accountId]
    )

    const available = (balances[0]?.posted ?? 0n) + (balances[0]?.pending ?? 0n)
    if (available + leg.amount < 0n) throw new Error("Insufficient funds")
  }

  return sorted
}
