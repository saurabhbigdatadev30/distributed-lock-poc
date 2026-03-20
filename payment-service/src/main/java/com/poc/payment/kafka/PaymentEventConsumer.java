package com.poc.payment.kafka;

import com.poc.payment.model.PaymentRecord;
import com.poc.payment.service.PaymentService;
import com.poc.shared.event.OrderCreatedEvent;
import com.poc.shared.event.PaymentSuccessEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DataIntegrityViolationException;
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
public class PaymentEventConsumer {

    private static final String PAYMENT_SUCCESS_TOPIC = "payment-success";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);
    private static final String IDEMPOTENCY_KEY_PREFIX = "payment:processed:";

    private final PaymentService paymentService;
    private final RedissonClient redissonClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = "order-created",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(@Payload OrderCreatedEvent event, Acknowledgment ack) {
        String orderId = event.getOrderId();
        String idempotencyKey = event.getIdempotencyKey();
        log.info("Received OrderCreatedEvent for orderId={}", orderId);

        // Redis-first idempotency check
        RBucket<String> idempotencyBucket = redissonClient.getBucket(IDEMPOTENCY_KEY_PREFIX + idempotencyKey);
        if (idempotencyBucket.isExists()) {
            log.info("Idempotency skip (Redis): idempotencyKey={} already processed by payment-service", idempotencyKey);
            ack.acknowledge();
            return;
        }

        RLock lock = redissonClient.getLock("payment:" + orderId);
        try {
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Could not acquire lock for payment:{}", orderId);
                ack.acknowledge();
                return;
            }
            log.info("Lock acquired for payment:{}", orderId);

            PaymentRecord record;
            try {
                record = paymentService.processPayment(event);
            } catch (DataIntegrityViolationException e) {
                log.warn("Idempotency skip (DB constraint): idempotencyKey={} already persisted", idempotencyKey);
                ack.acknowledge();
                return;
            }

            // Set Redis idempotency key with 24h TTL
            idempotencyBucket.set("processed", IDEMPOTENCY_TTL);
            log.info("Idempotency key set for {}{}", IDEMPOTENCY_KEY_PREFIX, idempotencyKey);

            // Publish PaymentSuccessEvent
            PaymentSuccessEvent successEvent = PaymentSuccessEvent.builder()
                    .orderId(orderId)
                    .amount(event.getAmount())
                    .transactionId(record.getTransactionId())
                    .build();
            kafkaTemplate.send(PAYMENT_SUCCESS_TOPIC, orderId, successEvent);
            log.info("Published PaymentSuccessEvent for orderId={}", orderId);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring lock for payment:{}", orderId, e);
        } catch (Exception e) {
            log.error("Unexpected error processing payment for orderId={}: {}", orderId, e.getMessage(), e);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("Lock released for payment:{}", orderId);
            }
            ack.acknowledge();
        }
    }
}
