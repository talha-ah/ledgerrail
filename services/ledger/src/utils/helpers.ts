export function randomString(length = 8) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  let result = ""
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return result
}

export function randomNumber(length = 3): number {
  if (length <= 0) return Math.random()

  // Pick a first digit from 1 to 9 so the number doesn't start with 0
  const firstDigit = Math.floor(Math.random() * 9) + 1

  // Generate the remaining digits
  let remainingDigits = ""
  for (let i = 1; i < length; i++) {
    remainingDigits += Math.floor(Math.random() * 10)
  }

  return Number(firstDigit + remainingDigits)
}
