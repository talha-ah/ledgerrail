// acks=all plus 2 in-sync replicas means a write is acknowledged only once a
// majority holds it — so losing two brokers costs write availability rather
// than durability. That is the trade, chosen deliberately.
export const MIN_INSYNC_REPLICAS = 2
export const DEFAULT_PARTITIONS = 3
export const DEFAULT_REPLICATION = 3

export const TOPICS = {
  LEDGER_EVENTS: "ledger.events",
}

export const GROUPS = {
  BALANCES_PROJECTION: "balances-projection",
}

export const accountKey = (accountId: number) => `account:${accountId}`
export const transactionKey = (eventId: string) => `transaction:${eventId}`
