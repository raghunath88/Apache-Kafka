package com.dwp.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents the top-level Kafka event envelope.
 * Maps 1:1 to the JSON schema enforced by the Node.js producer.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    private String eventId;
    private String eventType;
    private String eventVersion;
    private Instant timestamp;
    private OrderPayload payload;

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderPayload {
        private String userId;
        private String orderId;
        private String service;
        private Double amount;
        private String currency;
        private String status;
        private Metadata metadata;
    }

    @Data
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Metadata {
        private String ipAddress;
        private String userAgent;
        private String channel;
    }
}
