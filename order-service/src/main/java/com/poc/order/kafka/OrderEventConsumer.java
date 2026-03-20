package com.poc.order.kafka;

import com.poc.order.model.Order;
import static com.poc.order.model.Order.STATUS_CONFIRMED;
import com.poc.order.repository.OrderRepository;
import com.poc.shared.event.InventoryReservedEvent;
import com.poc.shared.event.PaymentSuccessEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderRepository orderRepository;
    private final RedissonClient redissonClient;

    @KafkaListener(
            topics = "inventory-reserved",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onInventoryReserved(@Payload InventoryReservedEvent event, Acknowledgment ack) {
        String orderId = event.getOrderId();
        log.info("Received InventoryReservedEvent for orderId={}", orderId);

        RLock lock = redissonClient.getLock("order:" + orderId);
        try {
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Could not acquire lock for order:{} on inventory-reserved", orderId);
                ack.acknowledge();
                return;
            }
            log.info("Lock acquired for order:{} (inventory-reserved)", orderId);

            orderRepository.findById(orderId).ifPresent(order -> {
                order.setInventoryReserved(true);
                if (order.isPaymentSuccess()) {
                    order.setStatus(STATUS_CONFIRMED);
                    log.info("Order {} CONFIRMED (both inventory+payment done)", orderId);
                }
                orderRepository.save(order);
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring lock for order:{}", orderId, e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for order:{} (inventory-reserved)", orderId);
            }
            ack.acknowledge();
        }
    }

    @KafkaListener(
            topics = "payment-success",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentSuccess(@Payload PaymentSuccessEvent event, Acknowledgment ack) {
        String orderId = event.getOrderId();
        log.info("Received PaymentSuccessEvent for orderId={}", orderId);

        RLock lock = redissonClient.getLock("order:" + orderId);
        try {
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Could not acquire lock for order:{} on payment-success", orderId);
                ack.acknowledge();
                return;
            }
            log.info("Lock acquired for order:{} (payment-success)", orderId);

            orderRepository.findById(orderId).ifPresent(order -> {
                order.setPaymentSuccess(true);
                if (order.isInventoryReserved()) {
                    order.setStatus(STATUS_CONFIRMED);
                    log.info("Order {} CONFIRMED (both inventory+payment done)", orderId);
                }
                orderRepository.save(order);
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring lock for order:{}", orderId, e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for order:{} (payment-success)", orderId);
            }
            ack.acknowledge();
        }
    }
}
