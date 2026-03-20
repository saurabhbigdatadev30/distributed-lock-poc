package com.poc.inventory.service;

import com.poc.inventory.model.InventoryItem;
import com.poc.inventory.repository.InventoryRepository;
import com.poc.shared.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    @Transactional
    public void reserveStock(OrderCreatedEvent event) {
        String sku = event.getSku();
        int requested = event.getQuantity();

        InventoryItem item = inventoryRepository.findById(sku)
                .orElseThrow(() -> new IllegalArgumentException("SKU not found: " + sku));

        if (item.getAvailableQuantity() < requested) {
            throw new IllegalStateException(
                    "Insufficient stock for SKU=" + sku +
                    " available=" + item.getAvailableQuantity() +
                    " requested=" + requested);
        }

        item.setAvailableQuantity(item.getAvailableQuantity() - requested);
        inventoryRepository.save(item);
        log.info("Reserved {} units of SKU={} for orderId={}", requested, sku, event.getOrderId());
    }
}
