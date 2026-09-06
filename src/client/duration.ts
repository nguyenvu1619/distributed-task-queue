import { InvalidInputError } from '../domain/errors';

/** Milliseconds, or a short form: `500ms`, `30s`, `5m`, `2h`, `1d`. */
export type Duration = number | string;

const UNITS: Record<string, number> = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

export function parseDuration(value: Duration): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) {
      throw new InvalidInputError(`Duration must be a non-negative number, got ${value}`);
    }
    return value;
  }

  const match = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/.exec(value.trim());
  if (!match) {
    throw new InvalidInputError(
      `Cannot parse duration "${value}" — expected a number of milliseconds or a value like "30s", "5m", "2h"`
    );
  }

  return Math.round(parseFloat(match[1]) * UNITS[match[2]]);
}
