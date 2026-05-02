require('dotenv').config();
require('express-async-errors');

const express = require('express');
const { connectProducer, disconnectProducer } = require('./kafka/producer');
const ordersRouter = require('./routes/orders');
const logger = require('./utils/logger');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// Health check — used by GitLab CI and AWS load balancer
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'dwp-kafka-producer', timestamp: new Date().toISOString() });
});

// Routes
app.use('/orders', ordersRouter);

// Global error handler
app.use((err, req, res, next) => {
  logger.error('Unhandled error', { error: err.message, stack: err.stack });
  res.status(500).json({ error: 'Internal server error', message: err.message });
});

async function start() {
  await connectProducer();
  logger.info('Kafka producer connected');

  const server = app.listen(PORT, () => {
    logger.info(`Producer service listening on port ${PORT}`);
  });

  // Graceful shutdown — critical for zero-downtime GitLab deploys
  const shutdown = async (signal) => {
    logger.info(`${signal} received — shutting down gracefully`);
    server.close(async () => {
      await disconnectProducer();
      logger.info('Producer disconnected. Goodbye.');
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

start().catch((err) => {
  logger.error('Failed to start producer service', { error: err.message });
  process.exit(1);
});

module.exports = app;
