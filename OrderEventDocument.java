package com.dwp.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;

/**
 * MongoDB document — stores the full raw event for audit, replay, and analytics.
 * MySQL holds the current state; MongoDB holds the immutable event history.
 */
@Document(collection = "order_events")
@CompoundIndex(name = "userId_timestamp", def = "{'userId': 1, 'timestamp': -1}")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEventDocument {

    @Id
    private String id;

    @Indexed(unique = true)
    @Field("eventId")
    private String eventId;             // idempotency key

    @Indexed
    @Field("userId")
    private String userId;

    @Field("orderId")
    private String orderId;

    @Field("eventType")
    private String eventType;

    @Field("eventVersion")
    private String eventVersion;

    @Field("timestamp")
    private Instant timestamp;

    @Field("payload")
    private Map<String, Object> payload;  // raw payload — schema-flexible for future events

    @Field("kafka")
    private KafkaMeta kafka;

    @Field("processedAt")
    private Instant processedAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaMeta {
        private String topic;
        private int partition;
        private long offset;
        private String groupId;
    }
}
