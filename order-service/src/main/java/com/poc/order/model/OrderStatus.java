package com.poc.order.model;

/**
 * Represents the lifecycle state of an {@link Order}.
 *
 * <pre>
 *  PENDING ──► CONFIRMED
 * </pre>
 *
 * PENDING   – order created; waiting for inventory + payment
 * CONFIRMED – both inventory reserved and payment succeeded
 */
public enum OrderStatus {
    PENDING,
    CONFIRMED
}