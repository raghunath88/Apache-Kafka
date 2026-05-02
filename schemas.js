const Ajv = require('ajv');
const ajv = new Ajv({ allErrors: true, coerceTypes: false });

/**
 * JSON Schema for DWP order events.
 * Validated before publishing to Kafka — acts as a lightweight schema registry.
 */
const orderEventSchema = {
  type: 'object',
  required: ['userId', 'orderId', 'service', 'amount', 'currency', 'status'],
  additionalProperties: false,
  properties: {
    userId: {
      type: 'string',
      pattern: '^usr-[a-zA-Z0-9-]+$',
      description: 'DWP user identifier — used as Kafka partition key',
    },
    orderId: {
      type: 'string',
      pattern: '^ord-[a-zA-Z0-9-]+$',
      description: 'Globally unique order identifier (use UUIDv4 prefixed with ord-)',
    },
    service: {
      type: 'string',
      enum: ['universal-credit', 'housing-benefit', 'personal-independence-payment', 'jobseekers-allowance', 'state-pension'],
      description: 'DWP benefit service type',
    },
    amount: {
      type: 'number',
      minimum: 0,
      maximum: 10000,
    },
    currency: {
      type: 'string',
      enum: ['GBP'],
    },
    status: {
      type: 'string',
      enum: ['SUBMITTED', 'PROCESSING', 'APPROVED', 'REJECTED', 'CANCELLED'],
    },
    metadata: {
      type: 'object',
      properties: {
        ipAddress: { type: 'string' },
        userAgent: { type: 'string' },
        channel: { type: 'string', enum: ['web', 'mobile', 'api', 'agent'] },
      },
      additionalProperties: false,
    },
  },
};

const validateOrderEvent = ajv.compile(orderEventSchema);

function validate(schema, data) {
  const valid = schema(data);
  if (!valid) {
    const errors = schema.errors.map(e => `${e.instancePath} ${e.message}`).join('; ');
    throw new Error(`Schema validation failed: ${errors}`);
  }
  return true;
}

module.exports = { validateOrderEvent: (data) => validate(validateOrderEvent, data) };
