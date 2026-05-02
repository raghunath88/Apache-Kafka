package com.dwp.kafka.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Relational representation of a DWP order — persisted to MySQL (AWS RDS).
 * Indexed on userId + status for the most common query patterns.
 */
@Entity
@Table(
    name = "orders",
    indexes = {
        @Index(name = "idx_user_id",           columnList = "user_id"),
        @Index(name = "idx_order_id",          columnList = "order_id", unique = true),
        @Index(name = "idx_user_status",       columnList = "user_id, status"),
        @Index(name = "idx_created_at",        columnList = "created_at"),
    }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id",   nullable = false, unique = true, length = 36)
    private String eventId;         // idempotency key — prevents duplicate processing

    @Column(name = "order_id",   nullable = false, unique = true, length = 36)
    private String orderId;

    @Column(name = "user_id",    nullable = false, length = 64)
    private String userId;

    @Column(name = "service",    nullable = false, length = 64)
    private String service;

    @Column(name = "amount",     nullable = false)
    private Double amount;

    @Column(name = "currency",   nullable = false, length = 3)
    private String currency;

    @Column(name = "status",     nullable = false, length = 32)
    private String status;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
        processedAt = Instant.now();
    }
}
