export const TOPICS = {
  LEDGER_EVENTS: "ledger.events",
}

export const accountKey = (accountId: number) => `account:${accountId}`
export const transactionKey = (eventId: string) => `transaction:${eventId}`
