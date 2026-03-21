package com.poc.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.order.model.Order;
import com.poc.order.model.OrderStatus;
import com.poc.order.outbox.OutboxEvent;
import com.poc.order.outbox.OutboxEventRepository;
import com.poc.order.repository.OrderRepository;
import com.poc.shared.dto.OrderRequest;
import com.poc.shared.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String ORDER_CREATED_TOPIC = "order-created";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;


    @Transactional
    public Order createOrder(String orderId, String idempotencyKey, OrderRequest request) {
        RLock lock = redissonClient.getLock("order:" + orderId);
        try {
            /**
             If the lock is available, it’s acquired immediately and held for 30 seconds for the processing time.
             If the lock is not available, it waits up to 5 seconds. If it can’t acquire the lock within
             that time,  it throws an exception to prevent duplicate processing.
             */
            boolean acquired = lock.tryLock(5, 30, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("Could not acquire lock for order:" + orderId);
            }
            log.info("Lock acquired for order:{}", orderId);

            // ── 1. Persist the Order ──────────────────────────────────────────────────
            Order order = Order.builder()
                    .orderId(orderId)
                    .sku(request.getSku())
                    .quantity(request.getQuantity())
                    .amount(request.getAmount())
                    .orderStatus(OrderStatus.PENDING)
                    .inventoryReserved(false)
                    .paymentSuccess(false)
                    .idempotencyKey(idempotencyKey)
                    .build();

            orderRepository.save(order);
            log.info("Order {} saved with status PENDING", orderId);

            // ── 2. Write the OutboxEvent in the SAME transaction ─────────────────────
            OrderCreatedEvent event = OrderCreatedEvent.builder()
                    .orderId(orderId)
                    .sku(request.getSku())
                    .quantity(request.getQuantity())
                    .amount(request.getAmount())
                    .idempotencyKey(idempotencyKey)
                    .build();

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .id(UUID.randomUUID().toString())
                    .aggregateId(orderId)
                    .aggregateType("Order")
                    .eventType(OrderCreatedEvent.class.getName())
                    .topic(ORDER_CREATED_TOPIC)
                    .payload(serialize(event))
                    .status(OutboxEvent.STATUS_PENDING)
                    .retryCount(0)
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(outboxEvent);
            log.info("OutboxEvent {} queued for order:{} – Kafka publish deferred to scheduler",
                    outboxEvent.getId(), orderId);

            // ── 3. Return; transaction commits here ──────────────────────────────────
            return order;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while acquiring lock for order:" + orderId, e);
        }
        finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for order:{}", orderId);
            }
        }
    }

    public Optional<Order> getOrder(String orderId) {
        return orderRepository.findById(orderId);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event payload", e);
        }
    }
}
