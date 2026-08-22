/**
 * Transient and permanent failures need opposite responses, and they arrive
 * through the same `throw`.
 *
 * A lock timeout or a dropped connection *will* succeed on retry, so retrying is
 * correct and a briefly blocked partition is the price of not losing data. A
 * malformed payload or an unknown event version will *never* succeed, so
 * retrying is the poison pill. Everything unrecognised is treated as transient:
 * a message wrongly retried costs latency, a message wrongly quarantined costs
 * correctness.
 */

export class PermanentError extends Error {
  readonly code?: string

  constructor(message: string, options?: { code?: string; cause?: unknown }) {
    super(message, { cause: options?.cause })
    this.name = "PermanentError"
    this.code = options?.code
  }
}

// Postgres groups SQLSTATEs by their first two characters.
//   22 — data exception (invalid text representation, numeric out of range …)
//   23 — integrity constraint violation (not-null, foreign key, check, unique)
// Both describe the *data*, so the same row will fail identically forever.
// Deliberately excluded: 40 (serialization failure, deadlock), 53 (insufficient
// resources), 57 (operator intervention, statement timeout) — all retryable.
const PERMANENT_SQLSTATE_CLASSES = new Set(["22", "23"])

export function errorCode(err: unknown): string | undefined {
  if (err instanceof PermanentError) return err.code

  if (typeof err === "object" && err !== null && "code" in err) {
    const code = (err as { code: unknown }).code
    if (typeof code === "string") return code
  }

  return undefined
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

export function isPermanent(err: unknown): boolean {
  if (err instanceof PermanentError) return true

  const code = errorCode(err)
  return code !== undefined && PERMANENT_SQLSTATE_CLASSES.has(code.slice(0, 2))
}

/**
 * Reachable only if a version was added to the union without being handled —
 * which is a compile error, which is the point. The throw is the runtime
 * backstop for the same mistake arriving through JSON.
 */
export function assertNever(x: never): never {
  throw new PermanentError(`unhandled case: ${JSON.stringify(x)}`)
}
