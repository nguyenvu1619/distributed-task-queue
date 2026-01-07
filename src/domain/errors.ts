export class InternalServerError extends Error {
  constructor(message: string = 'Internal Server Error') {
    super(message);
    this.name = 'InternalServerError';
  }
}

export class NotFoundError extends Error {
  constructor(message: string = 'Your requested Item is not found') {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class ConflictError extends Error {
  constructor(message: string = 'Your Item already exist') {
    super(message);
    this.name = 'ConflictError';
  }
}

export class BadParamInputError extends Error {
  constructor(message: string = 'Given Param is not valid') {
    super(message);
    this.name = 'BadParamInputError';
  }
}

