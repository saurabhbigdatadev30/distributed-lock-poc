package com.poc.order.service;

import com.poc.order.kafka.OrderEventProducer;
import com.poc.order.model.Order;
import com.poc.order.repository.OrderRepository;
import com.poc.shared.dto.OrderRequest;
import com.poc.shared.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;
    private final RedissonClient redissonClient;

    public Order createOrder(String orderId, String idempotencyKey, OrderRequest request) {
        RLock lock = redissonClient.getLock("order:" + orderId);
        try {
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("Could not acquire lock for order:" + orderId);
            }
            log.info("Lock acquired for order:{}", orderId);

            Order order = Order.builder()
                    .orderId(orderId)
                    .sku(request.getSku())
                    .quantity(request.getQuantity())
                    .amount(request.getAmount())
                    .status(Order.STATUS_PENDING)
                    .inventoryReserved(false)
                    .paymentSuccess(false)
                    .idempotencyKey(idempotencyKey)
                    .build();

            orderRepository.save(order);
            log.info("Order {} saved with status PENDING", orderId);

            OrderCreatedEvent event = OrderCreatedEvent.builder()
                    .orderId(orderId)
                    .sku(request.getSku())
                    .quantity(request.getQuantity())
                    .amount(request.getAmount())
                    .idempotencyKey(idempotencyKey)
                    .build();

            orderEventProducer.publishOrderCreated(event);
            return order;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while acquiring lock for order:" + orderId, e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for order:{}", orderId);
            }
        }
    }

    public Optional<Order> getOrder(String orderId) {
        return orderRepository.findById(orderId);
    }
}
