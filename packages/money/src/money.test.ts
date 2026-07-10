import fc from "fast-check"
import { expect, test } from "vitest"
import { money, add, subtract, parseMoney, formatMoney } from "./money.js"

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
