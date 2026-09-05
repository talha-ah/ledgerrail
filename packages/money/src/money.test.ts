import fc from "fast-check"
import { describe, it, expect, test } from "vitest"

import {
  add,
  money,
  convert,
  subtract,
  parseRate,
  parseMoney,
  formatMoney,
} from "./money.js"

test("Add: Invalid values throws", () => {
  expect(() => add(money(10n, "PKR"), money(20n, "GBP"))).toThrow(
    "Currencies should be same"
  )
})

test("Add: Returns correctly", () => {
  const result = add(money(10n, "PKR"), money(20n, "PKR"))

  expect(result.amount).toBe(30n)
})

test("Subtract Balance: check", () => {
  const result = subtract(money(10n, "PKR"), money(20n, "PKR"))

  expect(result.amount).toBe(-10n)
})

test("ParseMoney: valid", () => {
  expect(parseMoney("12.34", "PKR").amount).toBe(1234n)
  expect(parseMoney("12.3", "PKR").amount).toBe(1230n)
  expect(parseMoney("100", "JPY").amount).toBe(100n)
  expect(parseMoney("-0.05", "PKR").amount).toBe(-5n)
  expect(parseMoney("100", "PKR").amount).toBe(10000n)
  expect(parseMoney("1", "PKR").amount).toBe(100n)
})

test("ParseMoney: invalid", () => {
  expect(() => parseMoney("12.345", "PKR")).toThrow("Too many decimals")
})

test("FormatMoney: valid", () => {
  expect(formatMoney(money(5n, "PKR"))).toBe("0.05")
  expect(formatMoney(money(50n, "PKR"))).toBe("0.50")
  expect(formatMoney(money(-5n, "PKR"))).toBe("-0.05")
  expect(formatMoney(money(0n, "PKR"))).toBe("0.00")
})

test("ParseMoney+FormatMoney: Round tripe", () => {
  expect(formatMoney(parseMoney("12.34", "PKR"))).toBe("12.34")
  expect(formatMoney(parseMoney("100", "JPY"))).toBe("100")
})

test("format∘parse round-trips for all amounts", () => {
  fc.assert(
    fc.property(fc.bigInt(), (n) => {
      const m = money(n, "USD")
      expect(parseMoney(formatMoney(m), "USD").amount).toBe(n)
    })
  )
})

describe("convert", () => {
  it("lands in the target currency's own minor units", () => {
    // The case the naive multiplication gets wrong by 100x: JPY has no minor
    // unit, so $1.00 at 150 is ¥150, not ¥15,000.
    expect(convert(money(100n, "USD"), "150", "JPY")).toEqual({
      amount: 150n,
      currency: "JPY",
    })
  })

  it("shifts the other way too", () => {
    expect(convert(money(150n, "JPY"), "0.0066666667", "USD")).toEqual({
      amount: 100n,
      currency: "USD",
    })
  })

  it("truncates rather than rounding up", () => {
    // ₨2.00 at 0.0035 is 0.7 minor units of USD. Truncation is the choice; the
    // residual is what a rail booking FX to a ledger has to put somewhere.
    expect(convert(money(200n, "PKR"), "0.0035", "USD").amount).toBe(0n)
  })

  it("is the identity at rate 1 within a currency", () => {
    expect(convert(money(12_345n, "USD"), "1.0000000000", "USD").amount).toBe(
      12_345n
    )
  })

  it("refuses a rate that is not a rate", () => {
    expect(() => parseRate("1.2.3")).toThrow("malformed")
    expect(() => parseRate("")).toThrow("malformed")
    expect(() => parseRate("1.00000000001")).toThrow("too precise")
    expect(() => parseRate("12345678901")).toThrow("too large")
  })
})
