/**
 * =============================================================================
 * FRONTEND - LEARNING LAB FOR REDIS AND KAFKA
 * =============================================================================
 * 
 * This React app demonstrates:
 * 1. Redis caching (see data source change from cache vs database)
 * 2. Redis rate limiting (watch request counter)
 * 3. Redis leaderboards (real-time score updates)
 * 4. Kafka order processing (create orders, see events)
 * 
 * Open browser DevTools Console to see detailed logs
 * =============================================================================
 */

import React, { useState, useEffect } from 'react';
import axios from 'axios';

// API base URL
const API_URL = 'http://localhost:3001/api';

function App() {
  // Health status
  const [health, setHealth] = useState(null);

  // Products (Redis caching demo)
  const [products, setProducts] = useState([]);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [dataSource, setDataSource] = useState('');

  // Rate limiting demo
  const [rateLimitCount, setRateLimitCount] = useState(0);
  const [rateLimitRemaining, setRateLimitRemaining] = useState(10);

  // Leaderboard demo
  const [leaderboard, setLeaderboard] = useState([]);
  const [newUsername, setNewUsername] = useState('');
  const [newScore, setNewScore] = useState('');

  // Orders (Kafka demo)
  const [orders, setOrders] = useState([]);
  const [users, setUsers] = useState([]);
  const [selectedUser, setSelectedUser] = useState('');
  const [cart, setCart] = useState([]);

  // Kafka Flow Demo
  const [kafkaTopic, setKafkaTopic] = useState('demo-topic');
  const [kafkaMessage, setKafkaMessage] = useState('');
  const [sentMessages, setSentMessages] = useState([]);
  const [processedMessages, setProcessedMessages] = useState([]);
  const [kafkaFlowStep, setKafkaFlowStep] = useState(0); // 0=none, 1=sending, 2=sent, 3=retrieved, 4=processed

  // Logs
  const [logs, setLogs] = useState([]);

  // Notification
  const [notification, setNotification] = useState(null);

  // Add log entry
  const addLog = (message, type = 'info') => {
    const entry = {
      time: new Date().toLocaleTimeString(),
      message,
      type,
    };
    setLogs(prev => [entry, ...prev].slice(0, 50));
    console.log(`[${entry.time}] ${message}`);
  };

  // Show notification
  const showNotification = (message) => {
    setNotification(message);
    setTimeout(() => setNotification(null), 3000);
  };

  // Fetch health status
  const fetchHealth = async () => {
    try {
      const res = await axios.get(`${API_URL.replace('/api', '')}/health`);
      setHealth(res.data);
    } catch (err) {
      console.error('Health check failed:', err);
    }
  };

  // ===========================================================================
  // REDIS DEMO 1: CACHING
  // ===========================================================================
  
  // Fetch products
  const fetchProducts = async () => {
    addLog('Fetching all products...');
    try {
      const res = await axios.get(`${API_URL}/products`);
      setProducts(res.data.data);
      setDataSource(res.data.source);
      addLog(`Products loaded from ${res.data.source.toUpperCase()}`, 
             res.data.source === 'cache' ? 'success' : 'info');
    } catch (err) {
      addLog('Failed to fetch products', 'error');
    }
  };

  // Fetch single product
  const fetchProduct = async (id) => {
    addLog(`Fetching product ${id}...`);
    try {
      const res = await axios.get(`${API_URL}/products/${id}`);
      setSelectedProduct(res.data.data);
      setDataSource(res.data.source);
      addLog(`Product ${id} loaded from ${res.data.source.toUpperCase()}`,
             res.data.source === 'cache' ? 'success' : 'info');
    } catch (err) {
      addLog(`Failed to fetch product ${id}`, 'error');
    }
  };

  // Clear cache
  const clearCache = async () => {
    addLog('Clearing all cache...');
    try {
      await axios.delete(`${API_URL}/cache/clear`);
      addLog('Cache cleared!', 'success');
      showNotification('Cache cleared!');
    } catch (err) {
      addLog('Failed to clear cache', 'error');
    }
  };

  // ===========================================================================
  // REDIS DEMO 2: RATE LIMITING
  // ===========================================================================
  
  const testRateLimit = async () => {
    addLog('Testing rate limit...');
    try {
      const res = await axios.get(`${API_URL}/limited`);
      setRateLimitCount(prev => prev + 1);
      setRateLimitRemaining(res.data.requestsRemaining);
      addLog(`Rate limit: ${10 - res.data.requestsRemaining}/10 used`, 'success');
    } catch (err) {
      if (err.response?.status === 429) {
        addLog('Rate limit exceeded! Wait 60 seconds.', 'error');
        showNotification('Rate limit exceeded!');
      }
    }
  };

  // ===========================================================================
  // REDIS DEMO 3: LEADERBOARD
  // ===========================================================================
  
  const fetchLeaderboard = async () => {
    try {
      const res = await axios.get(`${API_URL}/leaderboard`);
      setLeaderboard(res.data);
    } catch (err) {
      addLog('Failed to fetch leaderboard', 'error');
    }
  };

  const updateScore = async (e) => {
    e.preventDefault();
    if (!newUsername || !newScore) return;

    addLog(`Updating score: ${newUsername} -> ${newScore}`);
    try {
      await axios.post(`${API_URL}/leaderboard`, {
        username: newUsername,
        score: parseInt(newScore),
      });
      addLog('Score updated!', 'success');
      setNewUsername('');
      setNewScore('');
      fetchLeaderboard();
    } catch (err) {
      addLog('Failed to update score', 'error');
    }
  };

  // ===========================================================================
  // KAFKA DEMO: ORDER PROCESSING
  // ===========================================================================
  
  const fetchUsers = async () => {
    try {
      const res = await axios.get(`${API_URL}/users`);
      setUsers(res.data);
    } catch (err) {
      addLog('Failed to fetch users', 'error');
    }
  };

  const fetchOrders = async () => {
    try {
      const res = await axios.get(`${API_URL}/orders`);
      setOrders(res.data);
    } catch (err) {
      addLog('Failed to fetch orders', 'error');
    }
  };

  const addToCart = (product) => {
    const existing = cart.find(item => item.productId === product.id);
    if (existing) {
      setCart(cart.map(item => 
        item.productId === product.id 
          ? { ...item, quantity: item.quantity + 1 }
          : item
      ));
    } else {
      setCart([...cart, {
        productId: product.id,
        name: product.name,
        price: parseFloat(product.price),
        quantity: 1,
      }]);
    }
    addLog(`Added ${product.name} to cart`);
  };

  const createOrder = async () => {
    if (!selectedUser || cart.length === 0) return;

    addLog(`Creating order for user ${selectedUser}...`);
    try {
      const res = await axios.post(`${API_URL}/orders`, {
        userId: parseInt(selectedUser),
        items: cart,
      });
      addLog(`Order #${res.data.order.id} created! Kafka event sent.`, 'success');
      showNotification(`Order #${res.data.order.id} created!`);
      setCart([]);
      fetchOrders();
    } catch (err) {
      addLog('Failed to create order', 'error');
    }
  };

  // ===========================================================================
  // KAFKA FLOW DEMO: SEND → RETRIEVE → PROCESS
  // ===========================================================================
  
  // Send message to Kafka
  const sendKafkaMessage = async () => {
    if (!kafkaMessage.trim()) return;

    setKafkaFlowStep(1); // Sending
    addLog(`📤 SENDING to Kafka topic "${kafkaTopic}"...`);

    try {
      const res = await axios.post(`${API_URL}/kafka/send`, {
        topic: kafkaTopic,
        message: { text: kafkaMessage, timestamp: Date.now() }
      });

      setKafkaFlowStep(2); // Sent
      addLog(`✅ Message SENT to Kafka! ID: ${res.data.message.id}`, 'success');
      
      // Refresh sent messages
      fetchSentMessages();
      
      // Wait a moment then check for processed
      setKafkaMessage('');
      
      // Auto-check for processed message after 1 second
      setTimeout(async () => {
        setKafkaFlowStep(3); // Retrieving
        addLog(`📥 RETRIEVING processed messages...`);
        await fetchProcessedMessages();
        setKafkaFlowStep(4); // Processed
        addLog(`✅ Message PROCESSED by consumer!`, 'success');
      }, 1000);

    } catch (err) {
      addLog('Failed to send Kafka message', 'error');
      setKafkaFlowStep(0);
    }
  };

  // Fetch sent messages
  const fetchSentMessages = async () => {
    try {
      const res = await axios.get(`${API_URL}/kafka/messages/sent`);
      setSentMessages(res.data.messages);
    } catch (err) {
      addLog('Failed to fetch sent messages', 'error');
    }
  };

  // Fetch processed messages
  const fetchProcessedMessages = async () => {
    try {
      const res = await axios.get(`${API_URL}/kafka/messages/processed`);
      setProcessedMessages(res.data.messages);
    } catch (err) {
      addLog('Failed to fetch processed messages', 'error');
    }
  };

  // Clear all Kafka messages
  const clearKafkaMessages = async () => {
    try {
      await axios.delete(`${API_URL}/kafka/messages`);
      setSentMessages([]);
      setProcessedMessages([]);
      setKafkaFlowStep(0);
      addLog('All Kafka messages cleared', 'success');
    } catch (err) {
      addLog('Failed to clear messages', 'error');
    }
  };

  // Initial load
  useEffect(() => {
    fetchHealth();
    fetchProducts();
    fetchLeaderboard();
    fetchUsers();
    fetchOrders();
    fetchSentMessages();
    fetchProcessedMessages();
  }, []);

  return (
    <div className="container">
      {/* Header */}
      <header>
        <h1>🔥 Redis & Kafka Learning Lab</h1>
        <p>Learn Redis caching, rate limiting, leaderboards, and Kafka messaging</p>
      </header>

      {/* Health Status */}
      <section className="section">
        <h3>📊 Service Status</h3>
        {health && (
          <div style={{ display: 'flex', gap: '20px' }}>
            {Object.entries(health.services).map(([name, status]) => (
              <span key={name}>
                <span className={`status ${status === 'connected' ? 'online' : 'offline'}`}></span>
                {name}: {status}
              </span>
            ))}
          </div>
        )}
      </section>

      {/* Main Grid */}
      <div className="grid">
        {/* Redis Caching Demo */}
        <section className="section">
          <h2>🚀 Redis Demo 1: Caching</h2>
          <p><small>Click products to see cache hits/misses</small></p>
          
          <button onClick={fetchProducts}>Refresh Products</button>
          <button onClick={clearCache} className="danger">Clear All Cache</button>
          
          <div className="product-grid" style={{ marginTop: '15px' }}>
            {products.map(product => (
              <div 
                key={product.id} 
                className="product-card"
                onClick={() => fetchProduct(product.id)}
                style={{ cursor: 'pointer' }}
              >
                <h4>{product.name}</h4>
                <div className="price">${product.price}</div>
                <small>Stock: {product.stock}</small>
              </div>
            ))}
          </div>

          {selectedProduct && (
            <div style={{ marginTop: '15px' }}>
              <h4>Selected Product:</h4>
              <span className={`badge ${dataSource}`}>
                {dataSource.toUpperCase()}
              </span>
              <pre>{JSON.stringify(selectedProduct, null, 2)}</pre>
            </div>
          )}
        </section>

        {/* Rate Limiting Demo */}
        <section className="section">
          <h2>⚡ Redis Demo 2: Rate Limiting</h2>
          <p><small>Click rapidly to test (10 requests/60s limit)</small></p>
          
          <button onClick={testRateLimit}>Send Request</button>
          
          <div style={{ marginTop: '15px' }}>
            <h4>Request Counter</h4>
            <div style={{ fontSize: '2rem', fontWeight: 'bold' }}>
              {rateLimitCount}/10
            </div>
            <p>Remaining: {rateLimitRemaining}</p>
            <div style={{ 
              background: '#2d3748', 
              height: '10px', 
              borderRadius: '5px',
              overflow: 'hidden',
            }}>
              <div style={{
                background: rateLimitCount >= 8 ? '#e53e3e' : '#38a169',
                width: `${(rateLimitCount / 10) * 100}%`,
                height: '100%',
                transition: 'width 0.3s',
              }} />
            </div>
          </div>
        </section>

        {/* Leaderboard Demo */}
        <section className="section">
          <h2>🏆 Redis Demo 3: Leaderboard</h2>
          <p><small>Sorted Sets for real-time rankings</small></p>
          
          <form onSubmit={updateScore} style={{ marginBottom: '15px' }}>
            <input
              placeholder="Username"
              value={newUsername}
              onChange={e => setNewUsername(e.target.value)}
            />
            <input
              type="number"
              placeholder="Score"
              value={newScore}
              onChange={e => setNewScore(e.target.value)}
            />
            <button type="submit" className="success">Update Score</button>
          </form>

          <button onClick={fetchLeaderboard} className="secondary">Refresh</button>

          <div style={{ marginTop: '15px' }}>
            {leaderboard.map((entry, index) => (
              <div key={entry.username} className="leaderboard-item">
                <span>
                  <span className="rank">#{entry.rank}</span>
                  {entry.username}
                </span>
                <span style={{ color: '#667eea', fontWeight: 'bold' }}>
                  {entry.score} pts
                </span>
              </div>
            ))}
            {leaderboard.length === 0 && <p>No entries yet. Add some scores!</p>}
          </div>
        </section>

        {/* Kafka Order Demo */}
        <section className="section">
          <h2>📦 Kafka Demo: Order Processing</h2>
          <p><small>Create orders → Kafka events → Processing</small></p>
          
          <div style={{ marginBottom: '15px' }}>
            <select 
              value={selectedUser} 
              onChange={e => setSelectedUser(e.target.value)}
              style={{
                background: '#2d3748',
                border: '1px solid #4a5568',
                color: '#fff',
                padding: '10px',
                borderRadius: '5px',
                width: '100%',
                marginBottom: '10px',
              }}
            >
              <option value="">Select User...</option>
              {users.map(user => (
                <option key={user.id} value={user.id}>{user.username}</option>
              ))}
            </select>
          </div>

          <h4>Products (click to add to cart):</h4>
          <div className="product-grid">
            {products.slice(0, 4).map(product => (
              <div 
                key={product.id}
                className="product-card"
                onClick={() => addToCart(product)}
                style={{ cursor: 'pointer' }}
              >
                <small>{product.name}</small>
                <div>${product.price}</div>
              </div>
            ))}
          </div>

          <h4>Cart:</h4>
          {cart.length > 0 ? (
            <>
              {cart.map(item => (
                <div key={item.productId} style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>{item.name} x{item.quantity}</span>
                  <span>${(item.price * item.quantity).toFixed(2)}</span>
                </div>
              ))}
              <button 
                onClick={createOrder} 
                className="success"
                style={{ marginTop: '10px', width: '100%' }}
                disabled={!selectedUser}
              >
                Create Order (Sends Kafka Event)
              </button>
            </>
          ) : (
            <p style={{ color: '#a0aec0' }}>Cart is empty</p>
          )}

          <h4 style={{ marginTop: '15px' }}>Recent Orders:</h4>
          <div style={{ maxHeight: '150px', overflow: 'auto' }}>
            {orders.slice(0, 5).map(order => (
              <div key={order.id} style={{ 
                display: 'flex', 
                justifyContent: 'space-between',
                background: '#0d1117',
                padding: '8px',
                borderRadius: '4px',
                margin: '5px 0',
              }}>
                <span>Order #{order.id}</span>
                <span style={{
                  color: order.status === 'created' ? '#38a169' : '#a0aec0',
                }}>
                  {order.status}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* Kafka Flow Demo - SEND → RETRIEVE → PROCESS */}
        <section className="section" style={{ gridColumn: 'span 2' }}>
          <h2>📨 Kafka Flow Demo: SEND → RETRIEVE → PROCESS</h2>
          <p><small>See the complete Kafka message flow in real-time</small></p>

          {/* Flow Steps Visual */}
          <div style={{ 
            display: 'flex', 
            justifyContent: 'space-between', 
            alignItems: 'center',
            background: '#0d1117',
            padding: '20px',
            borderRadius: '10px',
            marginBottom: '20px'
          }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '60px',
                height: '60px',
                borderRadius: '50%',
                background: kafkaFlowStep >= 1 ? '#3182ce' : '#2d3748',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto',
                fontSize: '24px'
              }}>📤</div>
              <p style={{ marginTop: '10px', fontWeight: kafkaFlowStep >= 1 ? 'bold' : 'normal' }}>
                1. SEND
              </p>
              <small style={{ color: '#a0aec0' }}>Producer</small>
            </div>

            <div style={{ fontSize: '24px', color: '#4a5568' }}>→</div>

            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '60px',
                height: '60px',
                borderRadius: '50%',
                background: kafkaFlowStep >= 2 ? '#6b46c1' : '#2d3748',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto',
                fontSize: '24px'
              }}>💾</div>
              <p style={{ marginTop: '10px', fontWeight: kafkaFlowStep >= 2 ? 'bold' : 'normal' }}>
                2. STORE
              </p>
              <small style={{ color: '#a0aec0' }}>Kafka Topic</small>
            </div>

            <div style={{ fontSize: '24px', color: '#4a5568' }}>→</div>

            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '60px',
                height: '60px',
                borderRadius: '50%',
                background: kafkaFlowStep >= 3 ? '#38a169' : '#2d3748',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto',
                fontSize: '24px'
              }}>📥</div>
              <p style={{ marginTop: '10px', fontWeight: kafkaFlowStep >= 3 ? 'bold' : 'normal' }}>
                3. RETRIEVE
              </p>
              <small style={{ color: '#a0aec0' }}>Consumer</small>
            </div>

            <div style={{ fontSize: '24px', color: '#4a5568' }}>→</div>

            <div style={{ textAlign: 'center' }}>
              <div style={{
                width: '60px',
                height: '60px',
                borderRadius: '50%',
                background: kafkaFlowStep >= 4 ? '#e53e3e' : '#2d3748',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto',
                fontSize: '24px'
              }}>⚙️</div>
              <p style={{ marginTop: '10px', fontWeight: kafkaFlowStep >= 4 ? 'bold' : 'normal' }}>
                4. PROCESS
              </p>
              <small style={{ color: '#a0aec0' }}>Transform</small>
            </div>
          </div>

          {/* Send Message Form */}
          <div style={{ marginBottom: '20px' }}>
            <div style={{ display: 'flex', gap: '10px', marginBottom: '10px' }}>
              <select 
                value={kafkaTopic}
                onChange={e => setKafkaTopic(e.target.value)}
                style={{
                  background: '#2d3748',
                  border: '1px solid #4a5568',
                  color: '#fff',
                  padding: '10px',
                  borderRadius: '5px',
                  flex: '0 0 200px'
                }}
              >
                <option value="demo-topic">demo-topic</option>
                <option value="orders">orders</option>
                <option value="notifications">notifications</option>
              </select>

              <input
                placeholder="Enter message (e.g., Hello Kafka!)"
                value={kafkaMessage}
                onChange={e => setKafkaMessage(e.target.value)}
                onKeyPress={e => e.key === 'Enter' && sendKafkaMessage()}
                style={{ flex: 1 }}
              />

              <button onClick={sendKafkaMessage} className="success">
                📤 Send to Kafka
              </button>
            </div>

            <button onClick={clearKafkaMessages} className="danger">
              Clear All Messages
            </button>
          </div>

          {/* Messages Display */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
            {/* Sent Messages */}
            <div>
              <h4 style={{ borderBottom: '2px solid #3182ce', paddingBottom: '10px' }}>
                📤 Sent Messages ({sentMessages.length})
              </h4>
              <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                {sentMessages.map((msg, i) => (
                  <div key={i} className="card" style={{ marginBottom: '10px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span className="badge" style={{ background: '#3182ce' }}>SENT</span>
                      <small>{new Date(msg.sentAt).toLocaleTimeString()}</small>
                    </div>
                    <pre style={{ fontSize: '11px', marginTop: '10px' }}>
                      {JSON.stringify(msg.message, null, 2)}
                    </pre>
                  </div>
                ))}
                {sentMessages.length === 0 && (
                  <p style={{ color: '#a0aec0', textAlign: 'center' }}>No messages sent yet</p>
                )}
              </div>
            </div>

            {/* Processed Messages */}
            <div>
              <h4 style={{ borderBottom: '2px solid #38a169', paddingBottom: '10px' }}>
                ⚙️ Processed Messages ({processedMessages.length})
              </h4>
              <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                {processedMessages.map((msg, i) => (
                  <div key={i} className="card" style={{ marginBottom: '10px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span className="badge" style={{ background: '#38a169' }}>PROCESSED</span>
                      <small>{new Date(msg.processedAt).toLocaleTimeString()}</small>
                    </div>
                    <div style={{ marginTop: '10px' }}>
                      <strong>Original:</strong>
                      <pre style={{ fontSize: '11px', color: '#a0aec0' }}>
                        {JSON.stringify(msg.message?.value, null, 2)}
                      </pre>
                      <strong>Transformed:</strong>
                      <pre style={{ fontSize: '11px', color: '#38a169' }}>
                        {JSON.stringify(msg.message?.result?.transformed, null, 2)}
                      </pre>
                    </div>
                  </div>
                ))}
                {processedMessages.length === 0 && (
                  <p style={{ color: '#a0aec0', textAlign: 'center' }}>No messages processed yet</p>
                )}
              </div>
            </div>
          </div>

          {/* Instructions */}
          <div style={{ 
            background: '#16213e', 
            padding: '15px', 
            borderRadius: '8px', 
            marginTop: '20px',
            fontSize: '14px'
          }}>
            <strong>How it works:</strong>
            <ol style={{ margin: '10px 0', paddingLeft: '20px' }}>
              <li><strong>SEND:</strong> Producer publishes message to Kafka topic</li>
              <li><strong>STORE:</strong> Kafka stores message in partition (durable)</li>
              <li><strong>RETRIEVE:</strong> Consumer reads message from topic</li>
              <li><strong>PROCESS:</strong> Consumer transforms/handles the message</li>
            </ol>
            <p style={{ margin: 0, color: '#a0aec0' }}>
              💡 Check the <strong>Activity Log</strong> below and backend logs (<code>docker logs lab-backend</code>) to see the full flow!
            </p>
          </div>
        </section>
      </div>

      {/* Activity Log */}
      <section className="section">
        <h2>📋 Activity Log</h2>
        <button onClick={() => setLogs([])} className="secondary">Clear Log</button>
        <div className="log" style={{ marginTop: '10px' }}>
          {logs.map((log, index) => (
            <div key={index} className={`log-entry ${log.type}`}>
              <strong>[{log.time}]</strong> {log.message}
            </div>
          ))}
          {logs.length === 0 && <p style={{ color: '#a0aec0' }}>No activity yet</p>}
        </div>
      </section>

      {/* Notification */}
      {notification && (
        <div className="notification">
          {notification}
        </div>
      )}
    </div>
  );
}

export default App;
