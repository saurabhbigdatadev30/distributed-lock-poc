package com.poc.order.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    private String orderId;

    private String sku;
    private int quantity;
    private double amount;


    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_CONFIRMED = "CONFIRMED";

    @Enumerated(EnumType.STRING)   // stored as "PENDING" / "CONFIRMED" in the DB column
    private OrderStatus orderStatus;

    /** PENDING | CONFIRMED */
    private String status;

    private boolean inventoryReserved;
    private boolean paymentSuccess;

    private String idempotencyKey;
}
