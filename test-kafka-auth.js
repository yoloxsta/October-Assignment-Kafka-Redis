// Test Kafka external connection with SASL authentication
// Run this on your LOCAL machine (not inside Docker)

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'test-client',
  brokers: ['localhost:9093'],
  // SASL Authentication - This is where you use the credentials
  sasl: {
    mechanism: 'plain',
    username: 'admin',
    password: 'admin-secret'
  },
  ssl: false, // SASL_PLAINTEXT doesn't use SSL
});

async function testConnection() {
  const producer = kafka.producer();
  
  try {
    console.log('Connecting to Kafka with SASL authentication...');
    console.log('Using: admin / admin-secret');
    await producer.connect();
    console.log('✅ SUCCESS! Connected to Kafka with credentials!');
    
    // Send a test message
    await producer.send({
      topic: 'demo-topic',
      messages: [{ value: 'Hello from external client with auth!' }],
    });
    console.log('✅ Message sent successfully!');
    
    await producer.disconnect();
  } catch (error) {
    console.error('❌ Connection failed:', error.message);
  }
}

testConnection();
