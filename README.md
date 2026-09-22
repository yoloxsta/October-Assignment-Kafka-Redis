# 🔥 Redis & Kafka Learning Lab

A complete Docker Compose setup for learning Redis and Kafka with a full-stack application (React + Node.js + PostgreSQL).

---

## 📋 Table of Contents

1. [What is Redis?](#what-is-redis)
2. [What is Kafka?](#what-is-kafka)
3. [Quick Start](#quick-start)
4. [Step-by-Step Testing Guide](#step-by-step-testing-guide)
5. [Redis Deep Dive](#redis-deep-dive)
6. [Kafka Deep Dive](#kafka-deep-dive)
7. [Troubleshooting](#troubleshooting)

---

## 🚀 Quick Start

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

## 📚 What is Redis?

**Redis** (Remote Dictionary Server) is an **in-memory key-value store**.

### Why Use Redis?

| Feature | Why It Matters |
|---------|----------------|
| **Speed** | ~1ms latency (100x faster than DB queries) |
| **Caching** | Reduce database load significantly |
| **Sessions** | Fast, auto-expiring session storage |
| **Rate Limiting** | Prevent API abuse |
| **Pub/Sub** | Simple real-time messaging |
| **Leaderboards** | Sorted sets for rankings |

### Redis Data Types

```
STRING  → "key": "value"
LIST    → "key": ["a", "b", "c"]
SET     → "key": {"a", "b", "c"} (unique)
ZSET    → "leaderboard": {player1: 100, player2: 200}
HASH    → "user:1": {name: "Alice", email: "alice@test.com"}
```

---

## 📨 What is Kafka?

**Apache Kafka** is a **distributed event streaming platform**.

### Why Use Kafka?

| Feature | Why It Matters |
|---------|----------------|
| **Decoupling** | Services don't need to know about each other |
| **Throughput** | Millions of messages per second |
| **Durability** | Messages persist on disk |
| **Replay** | Can reprocess historical events |
| **Scalability** | Partitioned across multiple brokers |

### Kafka Concepts

```
TOPIC       → Category of messages (like a folder)
PARTITION   → Split topic for parallelism
OFFSET      → Position in a partition (like a bookmark)
PRODUCER    → Sends messages to topics
CONSUMER    → Reads messages from topics
CONSUMER GROUP → Multiple consumers sharing the work
```

---

## 🎮 UI Testing Guide

### Open These URLs in Your Browser

| UI | URL | Purpose |
|----|-----|---------|
| **React App** | http://localhost:3000 | Interactive Redis & Kafka demos |
| **Kafka UI** | http://localhost:8080 | View topics, messages, consumers |
| **Backend API** | http://localhost:3001/health | API health check |

---

### React UI - Interactive Demos (http://localhost:3000)

#### 🔴 Redis Demo 1: Caching
1. Click any **product card** (Laptop, Mouse, etc.)
2. Watch the badge:
   - First click: `DATABASE` (red) = cache miss
   - Second click: `CACHE` (green) = cache hit!
3. Click **"Clear All Cache"** to reset

#### ⚡ Redis Demo 2: Rate Limiting
1. Click **"Send Request"** rapidly 10+ times
2. Watch the progress bar fill
3. After 10 requests: "Rate limit exceeded!"
4. Wait 60 seconds to reset

#### 🏆 Redis Demo 3: Leaderboard
1. Enter **username** and **score**
2. Click **"Update Score"**
3. Add more players - see real-time rankings!

#### 📦 Kafka Demo: Order Processing
1. **Select a user** from dropdown
2. **Click products** to add to cart
3. Click **"Create Order"** - sends Kafka event
4. Check **Activity Log** for events
5. Open **Kafka UI** to see the message!

---

### Kafka UI (http://localhost:8080)

1. Click **Topics** → **orders**
2. Click **Messages** tab
3. See all order events with full JSON
4. Create orders in React UI, refresh to see new messages

---

### Redis CLI (Direct Testing)

```powershell
# Open Redis CLI
docker exec -it lab-redis redis-cli

# Try these commands:
KEYS *                         # List all keys
GET product:1                  # See cached product
TTL product:1                  # Check expiration
ZREVRANGE leaderboard 0 9 WITHSCORES   # Leaderboard

exit
```

---

### Kafka CLI (Direct Testing)

```powershell
# Enter Kafka container
docker exec -it lab-kafka bash

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning

exit
```

---

## 🧪 Step-by-Step Testing Guide

### STEP 1: Verify All Services Are Running

```bash
# Check container status
docker-compose ps

# All should show "Up" or "running"
```

**Expected Output:**
```
NAME           STATUS    PORTS
lab-postgres   running   0.0.0.0:5432->5432/tcp
lab-redis      running   0.0.0.0:6379->6379/tcp
lab-zookeeper  running   2181/tcp
lab-kafka      running   0.0.0.0:9092-9093->9092-9093/tcp
lab-kafka-ui   running   0.0.0.0:8080->8080/tcp
lab-backend    running   0.0.0.0:3001->3001/tcp
lab-frontend   running   0.0.0.0:3000->3000/tcp
```

### STEP 2: Test Health Endpoint

```bash
curl http://localhost:3001/health
```

**Expected:**
```json
{
  "status": "ok",
  "services": {
    "postgres": "connected",
    "redis": "connected",
    "kafka": "connected"
  }
}
```

---

## 🔴 REDIS TESTING

### Test 1: Caching (See Cache Hit vs Miss)

**What:** Demonstrates how Redis caches database queries.

**Why:** Reduces database load and speeds up response times.

```bash
# First request - CACHE MISS (from database)
curl http://localhost:3001/api/products/1

# Second request - CACHE HIT (from Redis)
curl http://localhost:3001/api/products/1
```

**What to observe:**
- First request: `"source": "database"`
- Second request: `"source": "cache"` (much faster!)

**Clear cache and try again:**
```bash
curl -X DELETE http://localhost:3001/api/cache/clear
curl http://localhost:3001/api/products/1  # Cache miss again
```

---

### Test 2: Rate Limiting

**What:** Limits requests per time window using Redis.

**Why:** Prevents API abuse and protects resources.

```bash
# Run this command 11 times rapidly
curl http://localhost:3001/api/limited
```

**What happens:**
- Requests 1-10: Returns `"message": "Request allowed"`
- Request 11+: Returns `429 Too Many Requests`

**Redis command behind the scenes:**
```bash
INCR ratelimit:<client_ip>
EXPIRE ratelimit:<client_ip> 60
```

---

### Test 3: Session Storage

**What:** Store user sessions with auto-expiration.

**Why:** Sessions need to be fast and auto-expire for security.

```bash
# Create a session
curl -X POST http://localhost:3001/api/session \
  -H "Content-Type: application/json" \
  -d '{"userId": 1, "username": "alice"}'

# Response: {"sessionId": "session:1698123456789", ...}

# Retrieve session
curl http://localhost:3001/api/session/session:1698123456789
```

---

### Test 4: Leaderboards (Sorted Sets)

**What:** Real-time rankings using Redis Sorted Sets.

**Why:** Leaderboards need to be updated and queried instantly.

```bash
# Add scores
curl -X POST http://localhost:3001/api/leaderboard \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "score": 100}'

curl -X POST http://localhost:3001/api/leaderboard \
  -H "Content-Type: application/json" \
  -d '{"username": "bob", "score": 150}'

curl -X POST http://localhost:3001/api/leaderboard \
  -H "Content-Type: application/json" \
  -d '{"username": "charlie", "score": 200}'

# Get leaderboard
curl http://localhost:3001/api/leaderboard
```

**Expected Response:**
```json
[
  {"rank": 1, "username": "charlie", "score": 200},
  {"rank": 2, "username": "bob", "score": 150},
  {"rank": 3, "username": "alice", "score": 100}
]
```

---

### Test 5: Pub/Sub (Publish/Subscribe)

**What:** Simple real-time messaging.

**Why:** Lighter alternative to Kafka for simple notifications.

```bash
# Terminal 1: Subscribe to notifications
docker exec -it lab-redis redis-cli
SUBSCRIBE notifications

# Terminal 2: Publish a message
curl -X POST http://localhost:3001/api/notifications \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello from Redis Pub/Sub!"}'
```

---

### Test 6: Direct Redis CLI

Connect to Redis directly:

```bash
# Open Redis CLI
docker exec -it lab-redis redis-cli

# Try these commands:
SET mykey "Hello Redis"     # Store a value
GET mykey                    # Retrieve value
SETEX tempkey 10 "expires"   # Set with 10 second TTL
TTL tempkey                  # Check remaining time
DEL mykey                    # Delete key

# See all keys
KEYS *

# Check leaderboard
ZREVRANGE leaderboard 0 9 WITHSCORES

# Exit
exit
```

---

## 📨 KAFKA TESTING

### Test 1: Create Order (Producer)

**What:** Send an order event to Kafka.

**Why:** Decouples order creation from processing.

```bash
# Create an order
curl -X POST http://localhost:3001/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "items": [
      {"productId": 1, "quantity": 2, "price": 999.99},
      {"productId": 2, "quantity": 1, "price": 29.99}
    ]
  }'
```

**What happens:**
1. Order saved to PostgreSQL
2. Kafka event published to `orders` topic
3. Backend consumer receives and processes the event

---

### Test 2: View Kafka Messages (Kafka UI)

1. Open http://localhost:8080
2. Click on **Topics** → **orders**
3. Click **Messages** tab
4. You'll see your order events!

---

### Test 3: Kafka CLI Commands

```bash
# Enter Kafka container
docker exec -it lab-kafka bash

# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Create a new topic
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic test-topic --partitions 3 --replication-factor 1

# Describe a topic
kafka-topics --bootstrap-server localhost:9092 --describe \
  --topic orders

# Produce messages
kafka-console-producer --bootstrap-server localhost:9092 \
  --topic test-topic
> {"message": "Hello Kafka"}
> {"message": "Learning Kafka"}
> (Ctrl+C to exit)

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic test-topic --from-beginning

# Consume with key and value
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --property print.key=true \
  --property key.separator=":"

# Exit container
exit
```

---

### Test 4: Update Order Status

```bash
# Update order status (sends Kafka event)
curl -X PATCH http://localhost:3001/api/orders/1/status \
  -H "Content-Type: application/json" \
  -d '{"status": "shipped"}'

# Check Kafka UI for new event
```

---

## 🌐 Frontend Testing

1. Open http://localhost:3000
2. Try the interactive demos:
   - **Caching**: Click products to see cache hits/misses
   - **Rate Limiting**: Click button rapidly to hit limit
   - **Leaderboard**: Add scores and see real-time rankings
   - **Orders**: Create orders and watch Kafka events

---

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND (React)                        │
│                      http://localhost:3000                      │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        BACKEND (Node.js)                        │
│                      http://localhost:3001                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │    Redis     │  │  PostgreSQL  │  │    Kafka Producer    │  │
│  │   Client     │  │    Client    │  │    + Consumer        │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└─────────┼─────────────────┼─────────────────────┼──────────────┘
          │                 │                     │
          ▼                 ▼                     ▼
   ┌────────────┐    ┌────────────┐       ┌────────────┐
   │   REDIS    │    │ POSTGRESQL │       │   KAFKA    │
   │  Port 6379 │    │ Port 5432  │       │Port 9092/93│
   └────────────┘    └────────────┘       └─────┬──────┘
                                              │
                                              ▼
                                       ┌────────────┐
                                       │ ZOOKEEPER  │
                                       │ Port 2181  │
                                       └────────────┘
```

---

## 🔧 Troubleshooting

### Common Issues

**1. Port Already in Use**
```bash
# Check what's using the port
netstat -ano | findstr :3000

# Stop all containers and try again
docker-compose down
docker-compose up -d
```

**2. Kafka Not Ready**
```bash
# Kafka takes time to start. Wait and check:
docker-compose logs kafka | grep "started"

# Restart if needed
docker-compose restart kafka backend
```

**3. Backend Can't Connect**
```bash
# Check backend logs
docker-compose logs backend

# Restart backend
docker-compose restart backend
```

**4. Clear Everything and Start Fresh**
```bash
# Stop and remove all containers, networks, volumes
docker-compose down -v

# Rebuild and start
docker-compose up -d --build
```

### Useful Commands

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f backend

# Restart a service
docker-compose restart backend

# Execute command in container
docker exec -it lab-backend sh
docker exec -it lab-redis redis-cli
docker exec -it lab-kafka bash

# Check container resource usage
docker stats
```

---

## 📖 Further Learning

### Redis Resources
- [Redis Documentation](https://redis.io/docs/)
- [Redis Commands](https://redis.io/commands/)
- [Redis Use Cases](https://redis.com/redis-enterprise/technology/redis-use-cases/)

### Kafka Resources
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [KafkaJS (Node.js client)](https://kafka.js.org/)
- [Kafka UI](https://github.com/provectus/kafka-ui)

---

## 🎯 Summary

### When to Use Redis
- ✅ Caching frequently accessed data
- ✅ Session storage
- ✅ Rate limiting
- ✅ Real-time leaderboards
- ✅ Simple Pub/Sub

### When to Use Kafka
- ✅ Event sourcing
- ✅ Service decoupling
- ✅ High-throughput messaging
- ✅ Log aggregation
- ✅ Stream processing

---

## 🛑 Stop the Lab

```bash
# Stop all services
docker-compose down

# Stop and remove all data (volumes)
docker-compose down -v
```

---

Happy Learning! 🚀
