package com.example.common.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * OrderCreatedEvent — published by Order Service on topic "order-created".
 *
 * CONSUMERS (both run in parallel — Kafka fan-out):
 *   Inventory Service -> uses skuId + quantity -> lock "inventory:sku:{skuId}"
 *   Payment Service   -> uses orderId + amount -> lock "payment:{orderId}"
 *
 * Use BigDecimal for monetary values — never float/double (precision issues).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEvent {
    private String orderId;
    private String skuId;
    private int quantity;
    private BigDecimal amount;
    private String customerId;
    private Instant createdAt;
}