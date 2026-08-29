/**
 * Injectable logger. The library logs on ordinary outcomes — "queue saturated",
 * "group at its cap" — which flood stdout on any busy queue, so the sink has to
 * be the caller's choice.
 */
export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

export const consoleLogger: Logger = {
  debug: (message, ...args) => console.debug(message, ...args),
  info: (message, ...args) => console.info(message, ...args),
  warn: (message, ...args) => console.warn(message, ...args),
  error: (message, ...args) => console.error(message, ...args),
};

export const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

/** Prefixes every line with a component tag, e.g. `[worker:emails]`. */
export function prefixed(logger: Logger, prefix: string): Logger {
  return {
    debug: (message, ...args) => logger.debug(`${prefix} ${message}`, ...args),
    info: (message, ...args) => logger.info(`${prefix} ${message}`, ...args),
    warn: (message, ...args) => logger.warn(`${prefix} ${message}`, ...args),
    error: (message, ...args) => logger.error(`${prefix} ${message}`, ...args),
  };
}
