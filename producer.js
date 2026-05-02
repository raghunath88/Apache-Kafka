const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const logger = require('../utils/logger');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'dwp-producer',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 10,
    factor: 2,
    maxRetryTime: 30000,
  },
  // TLS for AWS MSK
  ...(process.env.KAFKA_USE_TLS === 'true' && {
    ssl: true,
    sasl: {
      mechanism: 'aws',
      authorizationIdentity: process.env.AWS_MSK_IAM_ROLE,
    },
  }),
});

// idempotent: true ensures exactly-once delivery per producer session
// acks: -1 waits for all in-sync replicas — critical for DWP financial data
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 5,
  transactionTimeout: 30000,
});

async function connectProducer() {
  await producer.connect();
}

async function disconnectProducer() {
  await producer.disconnect();
}

/**
 * Publish an event to a Kafka topic.
 *
 * @param {string} topic  - Target Kafka topic (e.g. 'dwp.orders')
 * @param {string} key    - Partition key — use userId for per-user ordering
 * @param {object} value  - Event payload (will be JSON-serialised)
 * @param {object} headers - Optional Kafka headers (traceId, source service, etc.)
 * @returns {object}      - Kafka record metadata (partition, offset, timestamp)
 */
async function publishEvent({ topic, key, value, headers = {} }) {
  const message = {
    key,
    value: JSON.stringify(value),
    headers: {
      'content-type': 'application/json',
      'produced-at': new Date().toISOString(),
      'producer-service': 'dwp-producer',
      ...headers,
    },
  };

  const recordMetadata = await producer.send({
    topic: topic || process.env.KAFKA_TOPIC || 'dwp.orders',
    compression: CompressionTypes.LZ4,
    messages: [message],
  });

  logger.info('Event published to Kafka', {
    topic,
    key,
    partition: recordMetadata[0].partition,
    offset: recordMetadata[0].offset,
  });

  return recordMetadata[0];
}

/**
 * Publish a batch of events in a single request — use for high-throughput scenarios.
 */
async function publishBatch({ topic, events }) {
  const messages = events.map(({ key, value, headers = {} }) => ({
    key,
    value: JSON.stringify(value),
    headers: {
      'content-type': 'application/json',
      'produced-at': new Date().toISOString(),
      ...headers,
    },
  }));

  return producer.send({
    topic,
    compression: CompressionTypes.LZ4,
    messages,
  });
}

module.exports = { connectProducer, disconnectProducer, publishEvent, publishBatch };
