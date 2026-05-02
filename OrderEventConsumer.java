package com.dwp.kafka.consumer;

import com.dwp.kafka.model.OrderEvent;
import com.dwp.kafka.service.OrderProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for DWP order events.
 *
 * Design decisions:
 *  - Manual offset acknowledgement (ack only after successful DB write)
 *  - Dead-letter logic delegated to DefaultErrorHandler in KafkaConsumerConfig
 *  - Idempotency enforced in OrderProcessingService via eventId uniqueness check
 *  - concurrency = 3 in config maps to 1 thread per partition
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final OrderProcessingService orderProcessingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics      = "${kafka.topic:dwp.orders}",
        groupId     = "${kafka.consumer.group-id:dwp-consumers}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String traceId = extractHeader(record, "x-trace-id");
        String eventType = extractHeader(record, "x-event-type");

        log.info("Received Kafka event [topic={}, partition={}, offset={}, key={}, eventType={}]",
            record.topic(), record.partition(), record.offset(), record.key(), eventType);

        try {
            OrderEvent event = objectMapper.readValue(record.value(), OrderEvent.class);

            orderProcessingService.process(event, record.partition(), record.offset());

            // Manually acknowledge ONLY after the DB write succeeds
            ack.acknowledge();

            log.info("Event processed and acknowledged [eventId={}, partition={}, offset={}, traceId={}]",
                event.getEventId(), record.partition(), record.offset(), traceId);

        } catch (Exception e) {
            // Do NOT ack — Spring's DefaultErrorHandler will retry up to 3 times,
            // then route to the dead-letter topic (dwp.orders.DLT)
            log.error("Failed to process event [partition={}, offset={}, traceId={}]: {}",
                record.partition(), record.offset(), traceId, e.getMessage(), e);
            throw new RuntimeException("Event processing failed", e);
        }
    }

    private String extractHeader(ConsumerRecord<?, ?> record, String headerName) {
        var header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value()) : "unknown";
    }
}
