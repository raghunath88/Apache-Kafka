package com.dwp.kafka.service;

import com.dwp.kafka.model.OrderEntity;
import com.dwp.kafka.model.OrderEvent;
import com.dwp.kafka.model.OrderEventDocument;
import com.dwp.kafka.repository.OrderRepository;
import com.dwp.kafka.repository.OrderEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;

/**
 * Core business logic for processing DWP order events from Kafka.
 *
 * Write strategy:
 *  1. Check for duplicate eventId (idempotency guard)
 *  2. Persist structured data to MySQL via JPA (source of truth for queries)
 *  3. Persist raw event document to MongoDB (immutable audit log / replay source)
 *
 * If either write fails, the exception propagates back to the consumer,
 * which will NOT acknowledge the offset — Kafka retries automatically.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProcessingService {

    private final OrderRepository orderRepository;
    private final OrderEventRepository orderEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void process(OrderEvent event, int partition, long offset) {
        String eventId = event.getEventId();

        // Idempotency check — if we already processed this eventId, skip gracefully
        // This handles at-least-once redelivery (e.g. after consumer rebalance)
        if (orderRepository.existsByEventId(eventId)) {
            log.warn("Duplicate event detected, skipping [eventId={}]", eventId);
            return;
        }

        OrderEvent.OrderPayload payload = event.getPayload();

        // 1. Write to MySQL (RDS) — structured, queryable state
        OrderEntity entity = OrderEntity.builder()
            .eventId(eventId)
            .orderId(payload.getOrderId())
            .userId(payload.getUserId())
            .service(payload.getService())
            .amount(payload.getAmount())
            .currency(payload.getCurrency())
            .status(payload.getStatus())
            .kafkaPartition(partition)
            .kafkaOffset(offset)
            .build();

        try {
            orderRepository.save(entity);
            log.debug("Order persisted to MySQL [orderId={}]", payload.getOrderId());
        } catch (DataIntegrityViolationException e) {
            // Race condition: another consumer thread wrote the same eventId between our check and insert
            log.warn("Concurrent duplicate write detected, skipping [eventId={}]", eventId);
            return;
        }

        // 2. Write to MongoDB — raw event document for audit + replay
        @SuppressWarnings("unchecked")
        Map<String, Object> payloadMap = objectMapper.convertValue(payload, Map.class);

        OrderEventDocument doc = OrderEventDocument.builder()
            .eventId(eventId)
            .userId(payload.getUserId())
            .orderId(payload.getOrderId())
            .eventType(event.getEventType())
            .eventVersion(event.getEventVersion())
            .timestamp(event.getTimestamp())
            .payload(payloadMap)
            .kafka(OrderEventDocument.KafkaMeta.builder()
                .topic("dwp.orders")
                .partition(partition)
                .offset(offset)
                .groupId("dwp-consumers")
                .build())
            .processedAt(Instant.now())
            .build();

        orderEventRepository.save(doc);
        log.debug("Event document persisted to MongoDB [eventId={}]", eventId);
    }
}
