const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { publishEvent } = require('../kafka/producer');
const { validateOrderEvent } = require('../kafka/schemas');
const logger = require('../utils/logger');

const router = express.Router();

/**
 * POST /orders
 * Validates the request body against the order schema,
 * then publishes the event to Kafka using the userId as partition key.
 *
 * Partition key = userId ensures all events for a given user
 * land on the same partition — guaranteeing ordering per user.
 */
router.post('/', async (req, res) => {
  const traceId = req.headers['x-trace-id'] || uuidv4();

  logger.info('Received order submission', { traceId, body: req.body });

  // Schema validation — fail fast before touching Kafka
  validateOrderEvent(req.body);

  const { userId, orderId, service, amount, currency, status, metadata } = req.body;

  const event = {
    eventId: uuidv4(),
    eventType: 'ORDER_SUBMITTED',
    eventVersion: '1.0',
    timestamp: new Date().toISOString(),
    payload: { userId, orderId, service, amount, currency, status, metadata },
  };

  const recordMetadata = await publishEvent({
    topic: process.env.KAFKA_TOPIC || 'dwp.orders',
    key: userId,               // partition key → all events for same user go to same partition
    value: event,
    headers: {
      'x-trace-id': traceId,
      'x-event-type': 'ORDER_SUBMITTED',
      'x-source': 'producer-service',
    },
  });

  logger.info('Order event published', {
    traceId,
    eventId: event.eventId,
    partition: recordMetadata.partition,
    offset: recordMetadata.offset,
  });

  res.status(202).json({
    status: 'accepted',
    eventId: event.eventId,
    traceId,
    kafka: {
      topic: process.env.KAFKA_TOPIC || 'dwp.orders',
      partition: recordMetadata.partition,
      offset: recordMetadata.offset,
    },
  });
});

/**
 * POST /orders/batch
 * High-throughput endpoint for bulk event ingestion.
 */
router.post('/batch', async (req, res) => {
  const { events } = req.body;

  if (!Array.isArray(events) || events.length === 0) {
    return res.status(400).json({ error: 'events must be a non-empty array' });
  }

  if (events.length > 500) {
    return res.status(400).json({ error: 'Maximum batch size is 500 events' });
  }

  // Validate all events before publishing any
  events.forEach((e, i) => {
    try {
      validateOrderEvent(e);
    } catch (err) {
      throw new Error(`Event at index ${i} is invalid: ${err.message}`);
    }
  });

  const published = await Promise.all(
    events.map(event =>
      publishEvent({
        topic: process.env.KAFKA_TOPIC || 'dwp.orders',
        key: event.userId,
        value: {
          eventId: uuidv4(),
          eventType: 'ORDER_SUBMITTED',
          eventVersion: '1.0',
          timestamp: new Date().toISOString(),
          payload: event,
        },
      })
    )
  );

  res.status(202).json({
    status: 'accepted',
    count: published.length,
    partitions: [...new Set(published.map(r => r.partition))],
  });
});

module.exports = router;
