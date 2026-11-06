import { LeaseLostError } from '../domain/errors';
import { SettleRetryOptions } from '../domain/worker';

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// SQLSTATEs that mean "not right now" rather than "no":
//   08 connection exception · 40 transaction rollback (serialization failure,
//   deadlock) · 53 insufficient resources (too many connections) · 57 operator
//   intervention (admin or crash shutdown, still starting up, statement timeout)
//   25006 read_only_sql_transaction: a session that lands on a standby after a
//   failover · 55P03 lock_not_available: lock_timeout fired
const TRANSIENT_SQLSTATE_CLASSES = new Set(['08', '40', '53', '57']);
const TRANSIENT_SQLSTATES = new Set(['25006', '55P03']);

/**
 * Whether a settle that failed with `error` is worth another attempt.
 *
 * The two ways of being wrong do not cost the same. Giving up on an error that
 * was transient is the bug retrying exists to fix: the job stays PROCESSING and
 * runs again once its lease expires. Retrying an error that was permanent only
 * costs time until the deadline. So anything not known to be permanent counts
 * as transient.
 */
export function isTransientSettleError(error: unknown): boolean {
  // The fence has answered: the job is already settled or someone else holds
  // it. Asking again gets the same answer.
  if (error instanceof LeaseLostError) {
    return false;
  }

  // Only an error the server sent has a severity, and then its code is a
  // SQLSTATE. Duck-typed rather than `instanceof DatabaseError`, which would
  // miss errors raised through another copy of `pg`.
  const { severity, code } = (error ?? {}) as { severity?: unknown; code?: unknown };
  if (typeof severity === 'string' && typeof code === 'string') {
    return TRANSIENT_SQLSTATE_CLASSES.has(code.slice(0, 2)) || TRANSIENT_SQLSTATES.has(code);
  }

  // No answer from the server: a refused or reset socket, a connect or read
  // timeout, a connection that ended mid-statement. node-postgres raises most
  // of these as plain Errors without a code, so they are not told apart.
  return true;
}

/**
 * Runs `settle` until it succeeds, fails with an error that is not transient,
 * or the next retry would start past the deadline — then rethrows the last
 * error. `settle` is handed the per-attempt statement timeout.
 *
 * Retrying is safe because every settle is fenced by lease_seq. An attempt
 * that failed, or timed out and was abandoned, may still have committed; the
 * next attempt then finds the lease gone and fails with LeaseLostError rather
 * than settling twice. The same holds if another worker has taken the job.
 */
export async function retrySettle(
  settle: (timeoutMs: number) => Promise<void>,
  options: SettleRetryOptions = {},
  onRetry: (error: unknown, attempt: number, delayMs: number) => void = () => {}
): Promise<void> {
  const deadlineMs = options.deadlineMs ?? 30_000;
  const attemptTimeoutMs = options.attemptTimeoutMs ?? 5_000;
  const baseDelayMs = options.baseDelayMs ?? 100;
  const maxDelayMs = options.maxDelayMs ?? 2_000;
  const giveUpAt = Date.now() + deadlineMs;

  for (let attempt = 1; ; attempt++) {
    try {
      await settle(attemptTimeoutMs);
      return;
    } catch (error) {
      // Full jitter: when the database comes back, every worker that was
      // waiting on it retries at once unless their delays are spread out.
      const delayMs = Math.round(Math.random() * Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1)));
      if (!isTransientSettleError(error) || Date.now() + delayMs >= giveUpAt) {
        throw error;
      }
      onRetry(error, attempt, delayMs);
      await sleep(delayMs);
    }
  }
}
