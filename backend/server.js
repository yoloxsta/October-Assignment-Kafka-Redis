/**
 * =============================================================================
 * BACKEND SERVER - LEARNING LAB FOR REDIS AND KAFKA
 * =============================================================================
 * 
 * This server demonstrates:
 * 1. REDIS USE CASES:
 *    - Caching (reduce database load)
 *    - Session storage (fast, auto-expiring)
 *    - Rate limiting (prevent abuse)
 *    - Pub/Sub (simple messaging)
 *    - Leaderboards (sorted sets)
 * 
 * 2. KAFKA USE CASES:
 *    - Event sourcing (log all changes)
 *    - Message queue (decouple services)
 *    - Real-time notifications
 *    - Order processing
 * =============================================================================
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { createClient } = require('redis');
const { Kafka } = require('kafkajs');

const app = express();
app.use(cors());
app.use(express.json());

// =============================================================================
// DATABASE CONNECTION (PostgreSQL)
// =============================================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// =============================================================================
// REDIS CONNECTION
// =============================================================================
/**
 * WHAT IS REDIS?
 * - Remote Dictionary Server
 * - In-memory key-value store (RAM-based, extremely fast)
 * - Supports strings, lists, sets, sorted sets, hashes, streams
 * 
 * WHY USE REDIS?
 * - Speed: ~1ms latency vs ~100ms for database queries
 * - Reduces database load by caching frequently accessed data
 * - Atomic operations (no race conditions)
 * - Built-in expiration (TTL - Time To Live)
 */
let redisClient;

async function connectRedis() {
  redisClient = createClient({
    url: process.env.REDIS_URL,
  });

  redisClient.on('error', (err) => console.error('Redis Error:', err));
  redisClient.on('connect', () => console.log('✅ Redis connected'));

  await redisClient.connect();
}

// =============================================================================
// KAFKA CONNECTION
// =============================================================================
/**
 * WHAT IS KAFKA?
 * - Distributed event streaming platform
 * - Handles high-throughput, real-time data feeds
 * - Messages are stored in topics (like folders)
 * - Producers write to topics, Consumers read from topics
 * 
 * WHY USE KAFKA?
 * - Decouples services (producer doesn't need to know about consumers)
 * - High throughput (millions of messages per second)
 * - Durable (messages persist on disk)
 * - Scalable (partitioned across multiple brokers)
 * 
 * KEY CONCEPTS:
 * - Topic: Category/feed of messages (e.g., "orders", "users")
 * - Partition: Split topic for parallelism
 * - Offset: Position of a message in a partition
 * - Consumer Group: Multiple consumers sharing the work
 */
let kafka;
let producer;
let consumer;

async function connectKafka() {
  kafka = new Kafka({
    clientId: 'lab-backend',
    brokers: [process.env.KAFKA_BROKERS],
  });

  // Producer: Sends messages to Kafka topics
  producer = kafka.producer();
  await producer.connect();
  console.log('✅ Kafka Producer connected');

  // Consumer: Reads messages from Kafka topics
  consumer = kafka.consumer({ groupId: 'lab-backend-group' });
  await consumer.connect();
  console.log('✅ Kafka Consumer connected');

  // Subscribe to topics we want to consume
  await consumer.subscribe({ topics: ['orders', 'notifications'], fromBeginning: true });

  // Start consuming messages
  await runConsumer();
}

// =============================================================================
// KAFKA CONSUMER - Process incoming messages
// =============================================================================
async function runConsumer() {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`\n📨 KAFKA MESSAGE RECEIVED:`);
      console.log(`   Topic: ${topic}`);
      console.log(`   Partition: ${partition}`);
      console.log(`   Offset: ${message.offset}`);
      console.log(`   Key: ${message.key?.toString()}`);
      console.log(`   Value: ${message.value?.toString()}`);

      // Process based on topic
      const data = JSON.parse(message.value.toString());
      
      if (topic === 'orders') {
        // Process order (e.g., update database, send email)
        console.log(`   📦 Processing order: ${data.orderId}`);
        
        // Update order status in database
        await pool.query(
          'UPDATE orders SET status = $1 WHERE id = $2',
          [data.status, data.orderId]
        );
      }
    },
  });
}

// =============================================================================
// KAFKA HELPER - Send message to topic
// =============================================================================
async function sendKafkaMessage(topic, key, value) {
  await producer.send({
    topic,
    messages: [
      {
        key,
        value: JSON.stringify(value),
        timestamp: Date.now().toString(),
      },
    ],
  });
  console.log(`📤 Kafka message sent to topic: ${topic}`);
}

// =============================================================================
// API ROUTES
// =============================================================================

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    services: {
      postgres: 'connected',
      redis: redisClient?.isOpen ? 'connected' : 'disconnected',
      kafka: producer ? 'connected' : 'disconnected',
    },
  });
});

// =============================================================================
// REDIS DEMONSTRATION ROUTES
// =============================================================================

/**
 * -------------------------------------------------------------------------
 * DEMO 1: CACHING
 * -------------------------------------------------------------------------
 * WHAT: Store frequently accessed data in memory
 * WHY: Reduce database queries, faster response times
 * HOW: Check Redis first, if not found, query DB and cache result
 * 
 * TTL (Time To Live): Auto-expire after N seconds
 * - Prevents stale data
 * - Saves memory
 * -------------------------------------------------------------------------
 */

// Get product with Redis caching
app.get('/api/products/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const cacheKey = `product:${id}`;

    // STEP 1: Try to get from Redis cache
    const cachedProduct = await redisClient.get(cacheKey);

    if (cachedProduct) {
      console.log(`🎯 CACHE HIT for product ${id}`);
      return res.json({
        source: 'cache',
        data: JSON.parse(cachedProduct),
      });
    }

    console.log(`❌ CACHE MISS for product ${id}`);

    // STEP 2: If not in cache, query database
    const result = await pool.query('SELECT * FROM products WHERE id = $1', [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }

    const product = result.rows[0];

    // STEP 3: Cache the result with TTL of 60 seconds
    await redisClient.setEx(cacheKey, 60, JSON.stringify(product));

    res.json({
      source: 'database',
      data: product,
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get all products with caching
app.get('/api/products', async (req, res) => {
  try {
    const cacheKey = 'products:all';
    const cached = await redisClient.get(cacheKey);

    if (cached) {
      console.log('🎯 CACHE HIT for all products');
      return res.json({ source: 'cache', data: JSON.parse(cached) });
    }

    console.log('❌ CACHE MISS for all products');
    const result = await pool.query('SELECT * FROM products');
    
    // Cache for 30 seconds
    await redisClient.setEx(cacheKey, 30, JSON.stringify(result.rows));

    res.json({ source: 'database', data: result.rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Clear cache for a product
app.delete('/api/cache/product/:id', async (req, res) => {
  const { id } = req.params;
  await redisClient.del(`product:${id}`);
  res.json({ message: `Cache cleared for product ${id}` });
});

/**
 * -------------------------------------------------------------------------
 * DEMO 2: RATE LIMITING
 * -------------------------------------------------------------------------
 * WHAT: Limit number of requests per time window
 * WHY: Prevent abuse, protect resources
 * HOW: Use Redis INCR with expiration
 * -------------------------------------------------------------------------
 */

// Rate-limited endpoint (10 requests per 60 seconds)
app.get('/api/limited', async (req, res) => {
  const clientId = req.ip || 'anonymous';
  const rateLimitKey = `ratelimit:${clientId}`;

  // Increment counter
  const requests = await redisClient.incr(rateLimitKey);

  // Set expiry on first request
  if (requests === 1) {
    await redisClient.expire(rateLimitKey, 60);
  }

  // Check limit
  if (requests > 10) {
    const ttl = await redisClient.ttl(rateLimitKey);
    return res.status(429).json({
      error: 'Too many requests',
      retryAfter: ttl,
    });
  }

  res.json({
    message: 'Request allowed',
    requestsRemaining: 10 - requests,
  });
});

/**
 * -------------------------------------------------------------------------
 * DEMO 3: SESSION STORAGE
 * -------------------------------------------------------------------------
 * WHAT: Store user session data
 * WHY: Fast access, auto-expiration
 * HOW: Use Redis Hash to store session data
 * -------------------------------------------------------------------------
 */

// Create session
app.post('/api/session', async (req, res) => {
  const { userId, username } = req.body;
  const sessionId = `session:${Date.now()}`;

  // Store session data as a hash
  await redisClient.hSet(sessionId, {
    userId,
    username,
    createdAt: Date.now().toString(),
  });

  // Expire after 1 hour (3600 seconds)
  await redisClient.expire(sessionId, 3600);

  res.json({ sessionId, message: 'Session created' });
});

// Get session
app.get('/api/session/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  const session = await redisClient.hGetAll(sessionId);

  if (Object.keys(session).length === 0) {
    return res.status(404).json({ error: 'Session not found or expired' });
  }

  res.json(session);
});

/**
 * -------------------------------------------------------------------------
 * DEMO 4: LEADERBOARDS (SORTED SETS)
 * -------------------------------------------------------------------------
 * WHAT: Maintain rankings with scores
 * WHY: Real-time leaderboards, rankings
 * HOW: Use Redis Sorted Sets (ZSET)
 * -------------------------------------------------------------------------
 */

// Update score
app.post('/api/leaderboard', async (req, res) => {
  const { username, score } = req.body;
  
  await redisClient.zAdd('leaderboard', [{ score, value: username }]);
  
  // Get updated rank
  const rank = await redisClient.zRevRank('leaderboard', username);
  
  res.json({ username, score, rank: rank + 1 });
});

// Get leaderboard
app.get('/api/leaderboard', async (req, res) => {
  // Get top 10 players (highest scores first)
  const top10 = await redisClient.zRangeWithScores('leaderboard', 0, 9, {
    REV: true,
  });

  res.json(top10.map((entry, index) => ({
    rank: index + 1,
    username: entry.value,
    score: entry.score,
  })));
});

/**
 * -------------------------------------------------------------------------
 * DEMO 5: PUB/SUB (SIMPLE MESSAGING)
 * -------------------------------------------------------------------------
 * WHAT: Publish/Subscribe messaging pattern
 * WHY: Real-time notifications, simpler than Kafka
 * HOW: PUBLISH to channel, SUBSCRIBE to receive messages
 * -------------------------------------------------------------------------
 */

// Publish a message
app.post('/api/notifications', async (req, res) => {
  const { message } = req.body;
  
  const notification = {
    message,
    timestamp: Date.now(),
  };

  // Publish to 'notifications' channel
  const subscribers = await redisClient.publish('notifications', JSON.stringify(notification));

  res.json({
    published: true,
    subscribersNotified: subscribers,
    notification,
  });
});

// =============================================================================
// KAFKA DEMONSTRATION ROUTES
// =============================================================================

/**
 * -------------------------------------------------------------------------
 * KAFKA DEMO: ORDER PROCESSING
 * -------------------------------------------------------------------------
 * WHAT: Send order events to Kafka
 * WHY: Decouple order creation from processing
 *      Multiple services can consume the same event:
 *      - Inventory service (update stock)
 *      - Email service (send confirmation)
 *      - Analytics service (track sales)
 * HOW: Producer sends message to 'orders' topic
 * -------------------------------------------------------------------------
 */

// Create order (sends Kafka event)
app.post('/api/orders', async (req, res) => {
  try {
    const { userId, items } = req.body;

    // Calculate total
    const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);

    // Create order in database
    const orderResult = await pool.query(
      'INSERT INTO orders (user_id, total_amount, status) VALUES ($1, $2, $3) RETURNING *',
      [userId, total, 'created']
    );

    const order = orderResult.rows[0];

    // Insert order items
    for (const item of items) {
      await pool.query(
        'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)',
        [order.id, item.productId, item.quantity, item.price]
      );
    }

    // Send Kafka event
    await sendKafkaMessage('orders', order.id.toString(), {
      orderId: order.id,
      userId,
      total,
      items,
      status: 'created',
      createdAt: order.created_at,
    });

    res.status(201).json({
      order,
      message: 'Order created and Kafka event sent',
    });
  } catch (error) {
    console.error('Error creating order:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update order status (sends Kafka event)
app.patch('/api/orders/:id/status', async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    await pool.query('UPDATE orders SET status = $1 WHERE id = $2', [status, id]);

    // Send Kafka event
    await sendKafkaMessage('orders', id, {
      orderId: parseInt(id),
      status,
      updatedAt: new Date().toISOString(),
    });

    res.json({ orderId: id, status, message: 'Order status updated' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get orders
app.get('/api/orders', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT o.*, u.username 
      FROM orders o 
      JOIN users u ON o.user_id = u.id 
      ORDER BY o.created_at DESC
    `);
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// =============================================================================
// UTILITY ROUTES
// =============================================================================

// Get all users
app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM users');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Clear all Redis data (for testing)
app.delete('/api/cache/clear', async (req, res) => {
  await redisClient.flushDb();
  res.json({ message: 'All cache cleared' });
});

// =============================================================================
// START SERVER
// =============================================================================
const PORT = process.env.PORT || 3001;

async function startServer() {
  try {
    // Connect to services
    await connectRedis();
    await connectKafka();

    // Start Express server
    app.listen(PORT, () => {
      console.log(`\n🚀 Server running on port ${PORT}`);
      console.log(`\n📚 API Endpoints:`);
      console.log(`   Redis Caching:`);
      console.log(`     GET  /api/products/:id   - Get product (cached)`);
      console.log(`     GET  /api/products       - Get all products (cached)`);
      console.log(`     DEL  /api/cache/product/:id - Clear product cache`);
      console.log(`\n   Rate Limiting:`);
      console.log(`     GET  /api/limited        - Rate-limited endpoint`);
      console.log(`\n   Sessions:`);
      console.log(`     POST /api/session        - Create session`);
      console.log(`     GET  /api/session/:id    - Get session`);
      console.log(`\n   Leaderboard:`);
      console.log(`     POST /api/leaderboard    - Update score`);
      console.log(`     GET  /api/leaderboard    - Get top 10`);
      console.log(`\n   Notifications (Pub/Sub):`);
      console.log(`     POST /api/notifications  - Publish notification`);
      console.log(`\n   Orders (Kafka):`);
      console.log(`     POST /api/orders         - Create order`);
      console.log(`     PATCH /api/orders/:id/status - Update status`);
      console.log(`     GET  /api/orders         - Get all orders`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
