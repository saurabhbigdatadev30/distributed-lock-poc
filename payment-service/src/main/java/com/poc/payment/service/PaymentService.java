package com.poc.payment.service;

import com.poc.payment.model.PaymentRecord;
import com.poc.payment.repository.PaymentRepository;
import com.poc.shared.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;

    @Transactional
    public PaymentRecord processPayment(OrderCreatedEvent event) {
        String transactionId = UUID.randomUUID().toString();

        PaymentRecord record = PaymentRecord.builder()
                .orderId(event.getOrderId())
                .amount(event.getAmount())
                .transactionId(transactionId)
                .idempotencyKey(event.getIdempotencyKey())
                .build();

        PaymentRecord saved = paymentRepository.save(record);
        log.info("Payment processed for orderId={} transactionId={}", event.getOrderId(), transactionId);
        return saved;
    }
}
