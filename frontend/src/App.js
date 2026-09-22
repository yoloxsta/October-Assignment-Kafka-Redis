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

  // Initial load
  useEffect(() => {
    fetchHealth();
    fetchProducts();
    fetchLeaderboard();
    fetchUsers();
    fetchOrders();
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
