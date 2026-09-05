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

const RATE_DECIMALS = 10
// Mirrors quote_rate NUMERIC(20,10) — a coupling worth naming rather than
// leaving implicit, since the two must move together.
const RATE_INTEGER_DIGITS = 10
const RATE_PATTERN = /^\d+(\.\d+)?$/
const RATE_SCALE = 10n ** BigInt(RATE_DECIMALS)

/**
 * A rate as a scaled integer, exported so a caller can reject a bad one at
 * wiring time. Discovering it inside a saga makes a configuration fault look
 * like a transient failure and burns a retry budget on a payment that was never
 * possible.
 */
export function parseRate(rate: string): bigint {
  if (!RATE_PATTERN.test(rate)) throw new Error(`Rate ${rate} is malformed`)

  const [whole, frac = ""] = rate.split(".")

  if (frac.length > RATE_DECIMALS)
    throw new Error(`Rate ${rate} is too precise`)

  // Parenthesised, and it has to be: `>` binds tighter than `||`, so
  // `whole?.length || 0 > N` means `whole?.length || (0 > N)` — "does it have an
  // integer part", true for every rate there is. tsc accepts it because
  // `number | boolean` is a fine condition.
  if ((whole?.length ?? 0) > RATE_INTEGER_DIGITS) {
    throw new Error(`Rate ${rate} is too large to store`)
  }

  return BigInt(whole + frac.padEnd(RATE_DECIMALS, "0"))
}

/**
 * Applies an exchange rate and lands in the target currency's own minor units.
 *
 * The shift is the part a caller cannot do for itself, and the reason this is
 * here rather than in whatever needs it: minor units are only comparable within
 * a currency, and a rate is quoted between *major* units. JPY has no minor unit
 * at all, so USD→JPY at 150 is 100 minor in and 150 minor out — a hundred times
 * less than multiplying the two integers suggests.
 *
 * Truncating, like every other division here. The fraction of a minor unit that
 * falls off is a rounding residual, and a rail that books FX to the ledger has
 * to put it somewhere rather than drop it.
 */
export function convert(m: Money, rate: string, to: Currency): Money {
  const shift = EXPONENT[to] - EXPONENT[m.currency]

  const scaled = m.amount * parseRate(rate)

  const numerator = shift > 0 ? scaled * 10n ** BigInt(shift) : scaled
  const denominator =
    shift < 0 ? RATE_SCALE * 10n ** BigInt(-shift) : RATE_SCALE

  return { amount: numerator / denominator, currency: to }
}
