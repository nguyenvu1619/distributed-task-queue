import { InvalidInputError } from '../domain/errors';

/**
 * Turns a payload into the `TEXT` column and back. Swap it per queue to use a
 * schema codec (zod, protobuf, …) instead of raw JSON.
 */
export interface Serializer<T = any> {
  serialize(value: T): string;
  deserialize(raw: string): T;
}

export const jsonSerializer: Serializer = {
  serialize(value) {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) {
      // JSON.stringify(undefined) is undefined, and payload is NOT NULL — fail
      // here with something readable rather than at the insert.
      throw new InvalidInputError('Job payload must be JSON-serializable and not undefined');
    }
    return encoded;
  },
  deserialize(raw) {
    return JSON.parse(raw);
  },
};
