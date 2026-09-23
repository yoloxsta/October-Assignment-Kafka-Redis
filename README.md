# Redis & Kafka Learning Lab

A complete Docker Compose setup for learning Redis and Kafka with a full-stack application (React + Node.js + PostgreSQL).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [What is Redis?](#what-is-redis)
3. [What is Kafka?](#what-is-kafka)
4. [Redis vs Kafka - When to Use What](#redis-vs-kafka---when-to-use-what)
5. [Quick Start](#quick-start)
6. [UI Testing Guide](#ui-testing-guide)
7. [API Testing Guide](#api-testing-guide)
8. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

### System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              YOUR BROWSER                                     │
│                    http://localhost:3000 (React UI)                          │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  │ HTTP Requests (REST API)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BACKEND (Node.js)                                  │
│                       http://localhost:3001                                  │
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │  Redis Client   │  │ PostgreSQL Pool │  │   Kafka Producer/Consumer   │ │
│  │                 │  │                 │  │                             │ │
│  │ - Caching       │  │ - Users         │  │ - Publish order events      │ │
│  │ - Rate Limit    │  │ - Products      │  │ - Consume order events      │ │
│  │ - Sessions      │  │ - Orders        │  │ - Process asynchronously    │ │
│  │ - Leaderboards  │  │                 │  │                             │ │
│  └────────┬────────┘  └────────┬────────┘  └──────────────┬──────────────┘ │
└───────────┼────────────────────┼──────────────────────────┼─────────────────┘
            │                    │                          │
            │                    │                          │
            ▼                    ▼                          ▼
     ┌──────────────┐    ┌──────────────┐          ┌──────────────┐
     │    REDIS     │    │  POSTGRESQL  │          │    KAFKA     │
     │  (In-Memory) │    │  (Disk-based)│          │  (Event Log) │
     │              │    │              │          │              │
     │ ~1ms         │    │ ~100ms       │          │ Durable      │
     │ - Key-Value  │    │ - Relational │          │ - Topics     │
     │ - TTL/Expire │    │ - ACID       │          │ - Partitions │
     │ - Pub/Sub    │    │ - Persistent │          │ - Offsets    │
     └──────────────┘    └──────────────┘          └──────┬───────┘
                                                        │
                                                        │
                                                        ▼
                                                 ┌──────────────┐
                                                 │  ZOOKEEPER   │
                                                 │ (Kafka Coord)│
                                                 └──────────────┘
```

### Data Flow Examples

#### 1. Redis Caching Flow
```
User Request -> Backend -> Check Redis Cache
                          +-- HIT: Return cached data (1ms)
                          +-- MISS: Query PostgreSQL -> Cache result -> Return (100ms)
```

#### 2. Kafka Order Processing Flow
```
User Creates Order -> Backend -> Save to PostgreSQL
                              -> Publish to Kafka "orders" topic
                              -> Return response to user

(Separate Process)
Kafka Consumer <- Reads from "orders" topic
               -> Process order (send email, update inventory, etc.)
               -> Multiple consumers can process same event independently
```

---

## What is Redis?

### Definition
**Redis** (Remote Dictionary Server) is an **in-memory data structure store** used as a database, cache, message broker, and streaming engine.

### Key Characteristics
| Characteristic | Description |
|----------------|-------------|
| **Speed** | All data in RAM -> ~1ms latency (100x faster than disk DB) |
| **Persistence** | Optional - can snapshot to disk |
| **Data Types** | Strings, Lists, Sets, Sorted Sets, Hashes, Streams |
| **Atomic** | All operations are atomic (thread-safe) |
| **TTL** | Built-in expiration for keys |

### Redis Data Types Explained

```
+-----------------------------------------------------------------+
| STRING - Simple key-value pair                                  |
| +-------------------------------------------------------------+ |
| | SET product:1 '{"name":"Laptop","price":999.99}'            | |
| | GET product:1  ->  '{"name":"Laptop","price":999.99}'        | |
| | SETEX product:1 60 "..."  ->  Expires in 60 seconds          | |
| +-------------------------------------------------------------+ |
|                                                                 |
| HASH - Like a JSON object (multiple fields in one key)         |
| +-------------------------------------------------------------+ |
| | HSET session:abc123 userId 1 username "alice"               | |
| | HGET session:abc123 username  ->  "alice"                    | |
| | HGETALL session:abc123  ->  {userId: "1", username: "alice"} | |
| +-------------------------------------------------------------+ |
|                                                                 |
| LIST - Ordered collection (like an array)                      |
| +-------------------------------------------------------------+ |
| | LPUSH queue:emails "email1" "email2"                        | |
| | RPOP queue:emails  ->  "email2"                              | |
| +-------------------------------------------------------------+ |
|                                                                 |
| SET - Unordered unique collection                              |
| +-------------------------------------------------------------+ |
| | SADD online:users "alice" "bob"                             | |
| | SMEMBERS online:users  ->  ["alice", "bob"]                  | |
| | SISMEMBER online:users "alice"  ->  1 (true)                 | |
| +-------------------------------------------------------------+ |
|                                                                 |
| ZSET (Sorted Set) - Ranking with scores                        |
| +-------------------------------------------------------------+ |
| | ZADD leaderboard 100 "alice" 200 "bob"                      | |
| | ZREVRANGE leaderboard 0 9 WITHSCORES  ->  Top 10             | |
| | ZRANK leaderboard "alice"  ->  1 (position)                  | |
| +-------------------------------------------------------------+ |
+-----------------------------------------------------------------+
```

### Redis Use Cases in This Lab

| Use Case | How It Works | Benefit |
|----------|--------------|---------|
| **Caching** | Store DB query results with TTL | Reduces DB load by 80%+, 100x faster responses |
| **Rate Limiting** | `INCR` counter per IP with 60s TTL | Prevents API abuse, protects resources |
| **Session Storage** | Hash with auto-expiration | Fast session lookup, automatic cleanup |
| **Leaderboards** | Sorted Sets (ZSET) | Real-time rankings, O(log N) updates |
| **Pub/Sub** | Publish/Subscribe channels | Simple real-time notifications |

---

## What is Kafka?

### Definition
**Apache Kafka** is a **distributed event streaming platform** for high-throughput, fault-tolerant messaging.

### Key Characteristics
| Characteristic | Description |
|----------------|-------------|
| **Throughput** | Millions of messages/second |
| **Durability** | Messages persist on disk |
| **Retention** | Configurable (days, weeks, forever) |
| **Replay** | Can reprocess historical events |
| **Scalability** | Partitioned across brokers |

### Kafka Concepts Visualized

```
+-----------------------------------------------------------------+
|                         KAFKA TOPIC: "orders"                   |
|                     (Like a folder for messages)                |
|                                                                 |
|  Partition 0          Partition 1          Partition 2          |
|  +--------------+    +--------------+    +--------------+      |
|  | Offset 0     |    | Offset 0     |    | Offset 0     |      |
|  | Offset 1     |    | Offset 1     |    | Offset 1     |      |
|  | Offset 2     |    | Offset 2     |    |              |      |
|  | Offset 3     |    |              |    |              |      |
|  +--------------+    +--------------+    +--------------+      |
|                                                                 |
|  Each partition is:                                             |
|  - Ordered (messages stay in sequence)                         |
|  - Immutable (can't change, only append)                       |
|  - Replicated (copies on multiple brokers)                     |
+-----------------------------------------------------------------+

+-----------------------------------------------------------------+
|                      PRODUCER -> TOPIC -> CONSUMERS               |
|                                                                 |
|  +----------+         +-------------+        +--------------+  |
|  | Producer |-------->|   Topic     |------->| Consumer 1   |  |
|  | (Backend)|         |  "orders"   |        | (Email Svc)  |  |
|  +----------+         +-------------+        +--------------+  |
|                               |               | Consumer 2   |  |
|                               |               | (Inventory)  |  |
|                               |               +--------------+  |
|                               |               | Consumer 3   |  |
|                               V               | (Analytics)  |  |
|                        Each consumer gets      +--------------+  |
|                        ALL messages (independent processing)   |
+-----------------------------------------------------------------+

+-----------------------------------------------------------------+
|                     CONSUMER GROUPS                             |
|                                                                 |
|  Consumer Group "email-service" (1 consumer = all partitions)  |
|  +------------+                                                 |
|  | Consumer A | <--- Partition 0 + 1 + 2 (all messages)        |
|  +------------+                                                 |
|                                                                 |
|  Consumer Group "order-processing" (3 consumers = 1 per part)  |
|  +------------+  +------------+  +------------+                |
|  | Consumer B |  | Consumer C |  | Consumer D |                |
|  +------------+  +------------+  +------------+                |
|        |               |               |                       |
|        V               V               V                       |
|   Partition 0     Partition 1     Partition 2                  |
|   (load balanced across consumers)                             |
+-----------------------------------------------------------------+
```

### Kafka Use Cases in This Lab

| Use Case | How It Works | Benefit |
|----------|--------------|---------|
| **Order Processing** | Order created -> Kafka event -> Multiple services consume | Decouples services, fault-tolerant |
| **Event Sourcing** | All order changes logged as events | Audit trail, can replay history |
| **Async Processing** | Email, inventory, analytics consume independently | Non-blocking, scalable |

---

## Redis vs Kafka - When to Use What

### Comparison Matrix

| Aspect | Redis | Kafka |
|--------|-------|-------|
| **Primary Use** | Cache, quick lookups | Event streaming, messaging |
| **Speed** | Sub-millisecond | Milliseconds |
| **Data Size** | Small (fits in RAM) | Unlimited (disk-based) |
| **Persistence** | Optional (can lose data) | Guaranteed (durable) |
| **Replay** | No | Yes (keep history) |
| **Ordering** | Not guaranteed | Guaranteed per partition |
| **Consumers** | Pub/Sub (all get message) | Consumer groups (load balanced) |

### Decision Tree

```
                    Need to cache data?
                    +-----------------+
                    |                 |
                   YES               NO
                    |                 |
                    V                 V
              +----------+    Need real-time messaging?
              |  REDIS   |    +---------------------+
              | (Caching)|    |                     |
              +----------+   YES                    NO
                             |                      |
                             V                      V
                     Need message history?    Use PostgreSQL
                     +-----------------+      (traditional DB)
                     |                 |
                    YES               NO
                     |                 |
                     V                 V
               +----------+     +----------+
               |  KAFKA   |     |  REDIS   |
               |(Durable) |     | (Pub/Sub)|
               +----------+     +----------+
```

### Use Case Examples

| Scenario | Use Redis When... | Use Kafka When... |
|----------|-------------------|-------------------|
| **Caching** | YES - Store frequently accessed data | NO - Not designed for caching |
| **Rate Limiting** | YES - Fast counter with TTL | NO - Overkill for simple counters |
| **Sessions** | YES - Fast, auto-expiring | NO - Too slow, wrong tool |
| **Notifications** | Simple, fire-and-forget | Complex, need history/audit |
| **Order Processing** | NO - No durability guarantee | YES - Durable, replayable |
| **Analytics** | Real-time counters | YES - Event stream for batch processing |
| **Log Aggregation** | NO - Not designed for this | YES - Perfect for log streams |

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Git (optional)

### Start All Services

```bash
# Start all services (first run takes ~2-3 minutes)
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f backend
```

### Access Points

| Service | URL | Purpose |
|---------|-----|---------|
| **Frontend** | http://localhost:3000 | React web UI |
| **Backend API** | http://localhost:3001 | Node.js REST API |
| **Kafka UI** | http://localhost:8080 | Visual Kafka manager |
| **PostgreSQL** | localhost:5432 | Database (user: labuser, pass: labpass123) |
| **Redis** | localhost:6379 | Redis CLI access |

---

## UI Testing Guide

### Open These URLs in Your Browser

| UI | URL | What You'll See |
|----|-----|-----------------|
| **React App** | http://localhost:3000 | Interactive Redis & Kafka demos |
| **Kafka UI** | http://localhost:8080 | Topics, messages, consumers |

---

### React UI Walkthrough (http://localhost:3000)

#### Demo 1: Redis Caching

**What's happening:**
```
1st Click -> Backend checks Redis -> NOT FOUND -> Query PostgreSQL -> Cache it -> Return
           |                       |
           +-- Cache MISS ---------+

2nd Click -> Backend checks Redis -> FOUND -> Return immediately
           |                       |
           +-- Cache HIT! ---------+
```

**Steps:**
1. Click any **product card** (Laptop, Mouse, etc.)
2. Observe the badge:
   - `DATABASE` = Data came from PostgreSQL (~100ms)
   - `CACHE` = Data came from Redis (~1ms)
3. Click **"Clear All Cache"** to reset and try again

---

#### Demo 2: Redis Rate Limiting

**What's happening:**
```
Request 1  ->  INCR ratelimit:127.0.0.1 -> 1  ->  EXPIRE 60s  ->  Allow
Request 2  ->  INCR ratelimit:127.0.0.1 -> 2  ->  Already set  ->  Allow
...
Request 10 ->  INCR ratelimit:127.0.0.1 -> 10 ->  Already set  ->  Allow
Request 11 ->  INCR ratelimit:127.0.0.1 -> 11 ->  Already set  ->  DENY (429)
```

**Steps:**
1. Click **"Send Request"** rapidly 10+ times
2. Watch the counter fill up
3. After 10 requests: Rate limit exceeded!
4. Wait 60 seconds for the TTL to expire

---

#### Demo 3: Redis Leaderboard (Sorted Sets)

**What's happening:**
```
ZADD leaderboard 100 "alice"   ->  Add alice with score 100
ZADD leaderboard 200 "bob"     ->  Add bob with score 200
ZREVRANGE leaderboard 0 9 WITHSCORES  ->  Get top 10, highest first
```

**Steps:**
1. Enter **username** and **score**
2. Click **"Update Score"**
3. Leaderboard updates in real-time (sorted by score)

---

#### Demo 4: Kafka Order Processing

**What's happening:**
```
+--------------------------------------------------------------+
|  User clicks "Create Order"                                  |
|           |                                                  |
|           V                                                  |
|  Backend saves order to PostgreSQL                           |
|           |                                                  |
|           V                                                  |
|  Backend publishes to Kafka "orders" topic                   |
|           |                                                  |
|           +-----> Kafka Consumer (in backend) receives       |
|           |         +-- Updates order status                 |
|           |                                                  |
|           V                                                  |
|  User sees "Order created and Kafka event sent"              |
+--------------------------------------------------------------+
```

**Steps:**
1. **Select a user** from dropdown (alice, bob, or charlie)
2. **Click products** to add to cart
3. Click **"Create Order (Sends Kafka Event)"**
4. Check the **Activity Log** at bottom for Kafka messages
5. Open **Kafka UI** (http://localhost:8080) to see the actual message

---

#### Demo 5: Kafka Flow Lab - SEND -> RETRIEVE -> PROCESS

This is the **core Kafka learning demo**. You can clearly see the complete message flow.

**What's happening:**
```
+--------------------------------------------------------------+
|  STEP 1: SEND                                                |
|  User types message -> Click "Send to Kafka"                 |
|           |                                                  |
|           V                                                  |
|  STEP 2: STORE                                               |
|  Backend publishes to Kafka "demo-topic"                     |
|           |                                                  |
|           V                                                  |
|  STEP 3: RETRIEVE                                            |
|  Kafka Consumer reads message from topic                     |
|           |                                                  |
|           V                                                  |
|  STEP 4: PROCESS                                             |
|  Consumer transforms message, adds metadata                  |
|           |                                                  |
|           V                                                  |
|  Frontend polls every 2s and updates UI                      |
+--------------------------------------------------------------+
```

**Steps:**
1. Find the **"Kafka Flow Demo: SEND -> RETRIEVE -> PROCESS"** section
2. Type a message in the input box
3. Click **"Send to Kafka"**
4. Watch the **flow steps** update (Sending -> Sent -> Retrieving -> Processed)
5. See the message appear in **Processed Messages** panel
6. Check the **Activity Log** for detailed steps

---

#### Demo 6: Manual Kafka Message (Kafka UI -> Backend -> Frontend)

This demonstrates **external message injection** - simulating another service publishing to Kafka.

**Steps:**

1. **Open Kafka UI**: http://localhost:8080

2. **Navigate to Topics**:
   - Click **Topics** -> **demo-topic**

3. **Produce a Message**:
   - Click **"Produce Message"** button
   - Enter this JSON:
     ```json
     {"text": "Hello from Kafka UI!", "userId": 123}
     ```
   - Click **"Submit"**

4. **Watch the Frontend** (http://localhost:3000):
   - Within **2 seconds**, a **green alert box** appears:
     ```
     +-------------------------------------------------------+
     |  New Message Received from Kafka!                     |
     |  1 new message(s) arrived at 3:45:30 PM  [Dismiss]    |
     |                                                       |
     |  Message:                                            |
     |  {                                                   |
     |    "text": "Hello from Kafka UI!",                   |
     |    "userId": 123                                     |
     |  }                                                   |
     +-------------------------------------------------------+
     ```
   - The **Processed Messages** panel auto-updates
   - The **Activity Log** shows: `1 new message(s) retrieved from Kafka!`

**This demonstrates:**
- Kafka UI acts as a **separate producer**
- Backend **consumer** picks up the message
- Frontend **polls** for updates every 2 seconds
- **Visual alert** appears for new messages

---

### Kafka UI Walkthrough (http://localhost:8080)

**Steps:**
1. Click **Topics** in left sidebar
2. Click **demo-topic** or **orders** topic
3. Click **Messages** tab
4. You'll see all events with full JSON payload
5. Create a new message (as shown in Demo 6), then refresh to see it

---

## API Testing Guide

### Redis Tests

```bash
# Test Caching
curl http://localhost:3001/api/products/1
# First: {"source":"database"} -> Second: {"source":"cache"}
```

---

### Kafka Flow Lab - SEND -> RETRIEVE -> PROCESS

This is the core Kafka learning demo. You can clearly see:
1. **SEND**: Producer publishes message to Kafka
2. **RETRIEVE**: Consumer reads message from Kafka
3. **PROCESS**: Consumer transforms/handles the message

#### Step 1: SEND a Message to Kafka

```bash
# Send a message to "demo-topic"
curl -X POST http://localhost:3001/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"topic":"demo-topic","message":{"text":"Hello Kafka!","userId":1}}'
```

**Response:**
```json
{
  "success": true,
  "flow": {
    "step1_send": "COMPLETED - Message sent to Kafka",
    "step2_store": "Kafka storing message in partition",
    "step3_retrieve": "Consumer will retrieve message",
    "step4_process": "Consumer will process message"
  },
  "message": {
    "id": "1790090875998",
    "topic": "demo-topic",
    "message": {"text": "Hello Kafka!", "userId": 1},
    "status": "sent"
  }
}
```

#### Step 2: Check SENT Messages

```bash
curl http://localhost:3001/api/kafka/messages/sent
```

#### Step 3: Check PROCESSED Messages

```bash
curl http://localhost:3001/api/kafka/messages/processed
```

**Response:**
```json
{
  "count": 1,
  "messages": [{
    "id": 1790090876042,
    "topic": "demo-topic",
    "message": {
      "key": "1790090875998",
      "value": {"text": "Hello Kafka!", "userId": 1},
      "result": {
        "type": "demo_processed",
        "original": {"text": "Hello Kafka!", "userId": 1},
        "transformed": {
          "text": "Hello Kafka!",
          "userId": 1,
          "processed": true,
          "processedAt": "2026-09-22T15:27:56.041Z",
          "consumerId": "lab-backend-consumer"
        }
      }
    },
    "status": "processed"
  }]
}
```

#### Step 4: See the Flow in Backend Logs

```bash
docker logs lab-backend 2>&1 | tail -50
```

You'll see:
```
SEND: Publishing to Kafka topic "demo-topic"
   Message sent to Kafka!

KAFKA MESSAGE RETRIEVED
   Topic: demo-topic
   Offset: 0
   
PROCESSING MESSAGE...
   Type: DEMO MESSAGE
   
STORING RESULT
   Status: processed
```

#### Step 5: Clear and Repeat

```bash
# Clear all messages
curl -X DELETE http://localhost:3001/api/kafka/messages

# Send again
curl -X POST http://localhost:3001/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"topic":"demo-topic","message":{"action":"test","data":"anything"}}'
```

---

### Kafka Flow Diagram

```
+-----------------------------------------------------------------------------+
|                        KAFKA MESSAGE FLOW                                    |
|                                                                              |
|  1. SEND (Producer)                                                          |
|  +----------+         +-------------------------------------+               |
|  |  Client  |-------->| POST /api/kafka/send               |               |
|  |          |         | {topic: "demo-topic", message: {}} |               |
|  +----------+         +------------------|------------------+               |
|                                          |                                   |
|                                          V                                   |
|                              +---------------------+                        |
|                              |   KAFKA PRODUCER    |                        |
|                              |   send() to topic   |                        |
|                              +----------|----------+                        |
|                                         |                                    |
|  2. STORE                                V                                    |
|                              +---------------------+                        |
|                              |   KAFKA BROKER      |                        |
|                              |   Topic: demo-topic |                        |
|                              |   Partition: 0      |                        |
|                              |   Offset: 0, 1, 2...|                        |
|                              +----------|----------+                        |
|                                         |                                    |
|  3. RETRIEVE                             V                                    |
|                              +---------------------+                        |
|                              |   KAFKA CONSUMER    |                        |
|                              |   eachMessage()     |                        |
|                              +----------|----------+                        |
|                                         |                                    |
|  4. PROCESS                              V                                    |
|                              +---------------------+                        |
|                              |   BUSINESS LOGIC    |                        |
|                              |   - Transform data  |                        |
|                              |   - Update DB       |                        |
|                              |   - Send emails     |                        |
|                              +----------|----------+                        |
|                                         |                                    |
|  5. STORE RESULT                         V                                    |
|                              +---------------------+                        |
|                              |   storeProcessed    |                        |
|                              |   Message()         |                        |
|                              +----------|----------+                        |
|                                         |                                    |
|                                         V                                    |
|  6. RETRIEVE RESULT         +-----------------------------+                |
|                             | GET /api/kafka/messages/    |                |
|                             |     processed               |                |
|                             +-----------------------------+                |
+-----------------------------------------------------------------------------+
```

---

### Real-World Example: Order Processing

```bash
# Create order (sends Kafka event)
curl -X POST http://localhost:3001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":1,"items":[{"productId":1,"quantity":2,"price":999.99}]}'

# The consumer will:
# 1. Receive order event
# 2. Update order status in database
# 3. Store result for retrieval
```

---

### Test Redis Caching

```bash
# First request - Cache MISS
curl http://localhost:3001/api/products/1
# Returns: {"source":"database","data":{...}}

# Second request - Cache HIT
curl http://localhost:3001/api/products/1
# Returns: {"source":"cache","data":{...}}

# Clear cache
curl -X DELETE http://localhost:3001/api/cache/clear
```

### Test Redis Rate Limiting

```bash
# Run 11 times rapidly
curl http://localhost:3001/api/limited

# First 10: {"message":"Request allowed","requestsRemaining":N}
# 11th: HTTP 429 Too Many Requests
```

### Test Redis Leaderboard

```bash
# Add scores
curl -X POST http://localhost:3001/api/leaderboard \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","score":100}'

curl -X POST http://localhost:3001/api/leaderboard \
  -H "Content-Type: application/json" \
  -d '{"username":"bob","score":200}'

# Get leaderboard
curl http://localhost:3001/api/leaderboard
```

### Test Kafka Order Processing

```bash
# Create order
curl -X POST http://localhost:3001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":1,"items":[{"productId":1,"quantity":2,"price":999.99}]}'

# Check Kafka UI at http://localhost:8080 to see the event
```

---

## Troubleshooting

### Common Issues

**Kafka not starting:**
```bash
# Kafka needs 60+ seconds. Wait and check:
docker-compose logs kafka

# Restart if needed
docker-compose restart kafka backend
```

**Port conflicts:**
```bash
# Check what's using ports
netstat -ano | findstr :3000

# Stop all and restart
docker-compose down
docker-compose up -d
```

**Fresh start:**
```bash
# Remove all containers, networks, volumes
docker-compose down -v

# Rebuild and start
docker-compose up -d --build
```

---

## Stop the Lab

```bash
# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

---

## Further Learning

- [Redis Documentation](https://redis.io/docs/)
- [Redis Commands](https://redis.io/commands/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [KafkaJS (Node.js client)](https://kafka.js.org/)

---

Happy Learning!
