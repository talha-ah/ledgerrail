export type Currency = "PKR" | "USD" | "EUR" | "GBP" | "JPY"

export interface Money {
  readonly amount: bigint
  readonly currency: Currency
}

const EXPONENT: Record<Currency, number> = {
  PKR: 2,
  USD: 2,
  EUR: 2,
  GBP: 2,
  JPY: 0,
}

export function money(amount: bigint, currency: Currency): Money {
  return { amount: amount, currency: currency }
}

export function parseMoney(decimal: string, currency: Currency): Money {
  const exponent = EXPONENT[currency]
  const sign = decimal.startsWith("-") ? "-" : ""

  const [whole, frac = ""] = decimal.replace("-", "").split(".") // We ignore any more digits and their values.

  if (frac.length > exponent) throw new Error("Too many decimals")

  const result = BigInt(sign + whole + frac.padEnd(exponent, "0"))

  return { amount: result, currency: currency }
}

export function formatMoney(m: Money): string {
  const exponent = EXPONENT[m.currency]

  if (exponent === 0) return String(m.amount)

  const neg = isNegative(m)
  const digits = String(neg ? -m.amount : m.amount).padStart(exponent + 1, "0")
  const out = digits.slice(0, -exponent) + "." + digits.slice(-exponent)

  return neg ? "-" + out : out
}

export function add(a: Money, b: Money): Money {
  if (a.currency !== b.currency) {
    throw new Error("Currencies should be same")
  }

  const result = a.amount + b.amount

  return { amount: result, currency: a.currency }
}

export function subtract(a: Money, b: Money): Money {
  if (a.currency !== b.currency) {
    throw new Error("Currencies should be same")
  }

  const result = a.amount - b.amount

  return { amount: result, currency: a.currency }
}

export function isNegative(a: Money): boolean {
  return a.amount < 0
}
