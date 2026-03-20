package com.poc.inventory.kafka;

import com.poc.inventory.service.InventoryService;
import com.poc.shared.event.InventoryReservedEvent;
import com.poc.shared.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventConsumer {

    private static final String INVENTORY_RESERVED_TOPIC = "inventory-reserved";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);
    private static final String IDEMPOTENCY_KEY_PREFIX = "inventory:processed:";

    private final InventoryService inventoryService;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = "order-created",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(@Payload OrderCreatedEvent event, Acknowledgment ack) {
        String orderId = event.getOrderId();
        String sku = event.getSku();
        log.info("Received OrderCreatedEvent for orderId={}, sku={}", orderId, sku);

        // Redis-first idempotency check
        RBucket<String> idempotencyBucket = redissonClient.getBucket(IDEMPOTENCY_KEY_PREFIX + orderId);
        if (idempotencyBucket.isExists()) {
            log.info("Idempotency skip: orderId={} already processed by inventory-service", orderId);
            ack.acknowledge();
            return;
        }

        RLock lock = redissonClient.getLock("inventory:sku:" + sku);
        try {
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Could not acquire lock for inventory:sku:{} (orderId={})", sku, orderId);
                ack.acknowledge();
                return;
            }
            log.info("Lock acquired for inventory:sku:{} (orderId={})", sku, orderId);

            inventoryService.reserveStock(event);

            // Mark as processed in Redis with 24h TTL
            idempotencyBucket.set("processed", IDEMPOTENCY_TTL);
            log.info("Idempotency key set for {}{}",IDEMPOTENCY_KEY_PREFIX, orderId);

            // Publish InventoryReservedEvent
            InventoryReservedEvent reservedEvent = InventoryReservedEvent.builder()
                    .orderId(orderId)
                    .sku(sku)
                    .quantity(event.getQuantity())
                    .build();
            kafkaTemplate.send(INVENTORY_RESERVED_TOPIC, orderId, reservedEvent);
            log.info("Published InventoryReservedEvent for orderId={}", orderId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring lock for inventory:sku:{}", sku, e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for inventory:sku:{} (orderId={})", sku, orderId);
            }
            ack.acknowledge();
        }
    }
}
