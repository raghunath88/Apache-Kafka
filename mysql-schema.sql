-- DWP Kafka Pipeline — MySQL Schema
-- Applied automatically on first docker-compose up

CREATE DATABASE IF NOT EXISTS dwp CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dwp;

CREATE TABLE IF NOT EXISTS orders (
    id              BIGINT          NOT NULL AUTO_INCREMENT,
    event_id        VARCHAR(36)     NOT NULL COMMENT 'Kafka event UUID — idempotency key',
    order_id        VARCHAR(36)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    service         VARCHAR(64)     NOT NULL COMMENT 'DWP benefit service type',
    amount          DECIMAL(10, 2)  NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'GBP',
    status          VARCHAR(32)     NOT NULL,
    kafka_partition SMALLINT,
    kafka_offset    BIGINT,
    created_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    processed_at    DATETIME(3),
    PRIMARY KEY (id),
    UNIQUE KEY uq_event_id  (event_id),
    UNIQUE KEY uq_order_id  (order_id),
    INDEX idx_user_id       (user_id),
    INDEX idx_user_status   (user_id, status),
    INDEX idx_created_at    (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
