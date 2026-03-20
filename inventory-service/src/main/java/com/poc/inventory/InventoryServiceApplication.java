package com.poc.inventory;

import com.poc.inventory.model.InventoryItem;
import com.poc.inventory.repository.InventoryRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }

    @Bean
    public CommandLineRunner seedInventory(InventoryRepository repository) {
        return args -> {
            repository.save(new InventoryItem("SKU-001", 100));
            repository.save(new InventoryItem("SKU-002", 50));
        };
    }
}
