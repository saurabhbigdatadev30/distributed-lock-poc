package com.poc.order.controller;

import com.poc.order.model.Order;
import com.poc.order.service.OrderService;
import com.poc.shared.dto.OrderRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        String idempotencyKey = UUID.randomUUID().toString();
        log.info("Received POST /api/orders for sku={}, generating orderId={}", request.getSku(), orderId);
        Order order = orderService.createOrder(orderId, idempotencyKey, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        log.info("Received GET /api/orders/{}", orderId);
        return orderService.getOrder(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
