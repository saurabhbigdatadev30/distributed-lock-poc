package com.poc.order.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Return all outbox events in the given status, oldest first.
     * The publisher uses this to drain the queue in order.
     */
    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(String status);
}

