# Distributed Lock POC

A **production-grade, Spring Boot multi-module Maven POC** demonstrating Kafka event-driven microservices with **Redisson distributed locks** and **idempotency**.

---

## Architecture

```
POST /api/orders
      │
      ▼ Lock: order:{orderId}
 ┌─────────────────────┐
 │    Order Service    │ → Saves PENDING order
 │      :8081          │ → Publishes order-created ──────────────────────┐
 └─────────────────────┘                                                  │
                                                                          ▼
                                         ┌─────────────────────────────────────────────┐
                                         │     Kafka: order-created (fan-out)          │
                                         └──────────┬──────────────────────────────────┘
                               ┌──────────────────  │  ──────────────────────────┐
                               ▼                                                  ▼
              ┌───────────────────────────┐              ┌───────────────────────────────────┐
              │    Inventory Service      │              │        Payment Service             │
              │         :8082             │              │             :8083                  │
              │                           │              │                                    │
              │ Lock: inventory:sku:{sku} │              │  Lock: payment:{orderId}           │
              │                           │              │  Redis idempotency key check       │
              │  → Reserve stock          │              │  DB unique constraint check        │
              │  → Update quantity        │              │  → Charge customer (simulated)     │
              └─────────────┬─────────────┘              └──────────────┬─────────────────────┘
                            │ inventory-reserved                         │ payment-success
                            └─────────────────────┐  ┌──────────────────┘
                                                   ▼  ▼
                                      ┌──────────────────────────┐
                                      │     Order Service        │
                                      │  Lock: order:{orderId}   │
                                      │  inventoryReserved=true  │
                                      │  paymentSuccess=true     │
                                      │  Status → CONFIRMED ✅   │
                                      └──────────────────────────┘
```

---

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Java 17**
- **Maven 3.8+**

---

## Project Structure

```
distributed-lock-poc/
├── pom.xml                     ← parent POM
├── docker-compose.yml          ← Kafka + Zookeeper + Redis
├── README.md
├── shared-library/             ← common DTOs, events
├── order-service/              ← :8081
├── inventory-service/          ← :8082
└── payment-service/            ← :8083
```

---

## How to Run

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** on port 2181
- **Kafka** on port 9092 (with topics `order-created`, `inventory-reserved`, `payment-success` pre-created)
- **Redis** on port 6379

### 2. Build the Project

```bash
mvn clean package -DskipTests
```

### 3. Start Each Service

Open three separate terminals:

```bash
# Terminal 1 – Order Service
cd order-service
mvn spring-boot:run

# Terminal 2 – Inventory Service
cd inventory-service
mvn spring-boot:run

# Terminal 3 – Payment Service
cd payment-service
mvn spring-boot:run
```

---

## How to Test

### Create an Order

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "sku": "SKU-001",
    "quantity": 5,
    "amount": 49.99
  }'
```

**Response:**
```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "sku": "SKU-001",
  "quantity": 5,
  "amount": 49.99,
  "status": "PENDING",
  "inventoryReserved": false,
  "paymentSuccess": false,
  "idempotencyKey": "7c9e6679-7425-40de-944b-e07fc1f90ae7"
}
```

### Check Order Status

```bash
curl http://localhost:8081/api/orders/550e8400-e29b-41d4-a716-446655440000
```

After a moment (once inventory and payment events have been processed), status becomes `CONFIRMED`:

```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "sku": "SKU-001",
  "quantity": 5,
  "amount": 49.99,
  "status": "CONFIRMED",
  "inventoryReserved": true,
  "paymentSuccess": true,
  "idempotencyKey": "7c9e6679-7425-40de-944b-e07fc1f90ae7"
}
```

### H2 Console

| Service | URL |
|---------|-----|
| Order   | http://localhost:8081/h2-console (JDBC: `jdbc:h2:mem:orderdb`) |
| Inventory | http://localhost:8082/h2-console (JDBC: `jdbc:h2:mem:inventorydb`) |
| Payment | http://localhost:8083/h2-console (JDBC: `jdbc:h2:mem:paymentdb`) |

---

## Distributed Lock Strategy

| Service | Lock Key | Purpose |
|---------|----------|---------|
| Order Service (create) | `order:{orderId}` | Prevent duplicate order creation |
| Order Service (consumer) | `order:{orderId}` | Atomic flag update + status transition |
| Inventory Service | `inventory:sku:{sku}` | Prevent overselling same SKU concurrently |
| Payment Service | `payment:{orderId}` | Prevent duplicate payment processing |

All locks use **Redisson `RLock`** with:
- **tryLock(5s wait, 10s lease)** — non-blocking acquisition with timeout
- **`finally` block** — lock release is always guaranteed
- **`isHeldByCurrentThread()`** check — safe to call unlock only if this thread holds it

---

## Idempotency Mechanism

### Inventory Service
- **Redis-first**: Before acquiring the lock, checks `inventory:processed:{orderId}` in Redis.
  - If key exists → log and skip (ack message).
  - After processing → set key with **24h TTL** using `RBucket`.

### Payment Service (Dual Idempotency)
1. **Redis check**: `payment:processed:{idempotencyKey}` — fast path, skips DB entirely.
2. **DB unique constraint**: `PaymentRecord.idempotencyKey` column is `UNIQUE`.
   - `DataIntegrityViolationException` is caught to handle the rare race condition where Redis TTL expired but the record exists in DB.
3. After successful processing → set Redis key with **24h TTL**.

---

## Kafka Topics

| Topic | Partitions | Publisher | Consumer(s) |
|-------|-----------|-----------|-------------|
| `order-created` | 3 | order-service | inventory-service, payment-service |
| `inventory-reserved` | 3 | inventory-service | order-service |
| `payment-success` | 3 | payment-service | order-service |

All consumers use **manual acknowledgment** (`AckMode.MANUAL_IMMEDIATE`) — messages are only acknowledged after successful processing.

---

## Pre-loaded Inventory Data

| SKU | Initial Quantity |
|-----|-----------------|
| SKU-001 | 100 |
| SKU-002 | 50 |
