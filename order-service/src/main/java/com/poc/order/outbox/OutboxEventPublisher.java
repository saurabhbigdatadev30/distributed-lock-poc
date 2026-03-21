package com.poc.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private static final int MAX_RETRY = 3;
    private static final String PUBLISHER_LOCK_KEY = "outbox:publisher:lock";

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void publishPendingEvents() {
        RLock lock = redissonClient.getLock(PUBLISHER_LOCK_KEY);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!acquired) {
                log.debug("Outbox publisher skipped: lock not acquired");
                return;
            }

            List<OutboxEvent> pendingEvents =
                    outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxEvent.STATUS_PENDING);

            for (OutboxEvent event : pendingEvents) {
                try {
                    Class<?> eventClass = Class.forName(event.getEventType());
                    Object payload = objectMapper.readValue(event.getPayload(), eventClass);

                    kafkaTemplate.send(event.getTopic(), event.getAggregateId(), payload)
                            .get(10, TimeUnit.SECONDS);

                    event.setStatus(OutboxEvent.STATUS_PUBLISHED);
                    event.setProcessedAt(LocalDateTime.now());
                    // Status is now processed , so these records wont be processed again
                    outboxEventRepository.save(event);

                    log.info("Outbox event published: id={} topic={} aggregateId={}",
                            event.getId(), event.getTopic(), event.getAggregateId());
                } catch (Exception ex) {
                    int attempts = event.getRetryCount() + 1;
                    event.setRetryCount(attempts);

                    if (attempts >= MAX_RETRY) {
                        event.setStatus(OutboxEvent.STATUS_FAILED);
                        event.setProcessedAt(LocalDateTime.now());
                        log.error("Outbox event marked FAILED after retries: id={} retries={}",
                                event.getId(), attempts, ex);
                    } else {
                        log.warn("Outbox event publish failed, will retry: id={} attempt={}",
                                event.getId(), attempts, ex);
                    }

                    outboxEventRepository.save(event);
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Outbox publisher interrupted", ex);
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}

