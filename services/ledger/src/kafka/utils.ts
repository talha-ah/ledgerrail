export const DEFAULT_PARTITIONS = 3
export const DEFAULT_REPLICATION = 1

export const TOPICS = {
  LEDGER_EVENTS: "ledger.events",
}

export const GROUPS = {
  BALANCES_PROJECTION: "balances-projection",
}

export const accountKey = (accountId: number) => `account:${accountId}`
export const transactionKey = (eventId: string) => `transaction:${eventId}`
