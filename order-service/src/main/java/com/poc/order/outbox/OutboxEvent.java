package com.poc.order.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Outbox table row: written atomically with the business entity in the same DB transaction.
 * A background scheduler reads PENDING rows and publishes them to Kafka.
 */
@Entity
@Table(name = "outbox_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    public static final String STATUS_PENDING   = "PENDING";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String STATUS_FAILED    = "FAILED";

    /** UUID – primary key */
    @Id
    private String id;

    /** Business aggregate id (e.g. orderId) – used as the Kafka message key */
    private String aggregateId;

    /** Human-readable aggregate type, e.g. "Order" */
    private String aggregateType;

    /**
     * Fully-qualified class name of the event, e.g.
     * "com.poc.shared.event.OrderCreatedEvent".
     * Used by the publisher to deserialize the payload back to the correct type.
     */
    private String eventType;

    /** Target Kafka topic */
    private String topic;

    /** JSON-serialized event payload */
    @Column(columnDefinition = "TEXT")
    private String payload;

    /** PENDING | PUBLISHED | FAILED */
    private String status;

    /** Number of failed publish attempts */
    private int retryCount;

    private LocalDateTime createdAt;

    /** Set when the event is successfully published or permanently failed */
    private LocalDateTime processedAt;
}

