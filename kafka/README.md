# Kafka SASL/PLAIN Authentication

## Users and Passwords

| Username | Password | Role | Purpose |
|----------|----------|------|---------|
| `admin` | `admin-secret` | Admin | Kafka UI, admin operations |
| `backend` | `backend-secret` | Producer/Consumer | Backend API service |
| `producer` | `producer-secret` | Producer | External producers |
| `consumer` | `consumer-secret` | Consumer | External consumers |

## How Authentication Works

```
+------------------------------------------------------------------+
|                    SASL/PLAIN FLOW                                |
|                                                                  |
|  1. Client connects to Kafka                                     |
|  2. Kafka requests authentication                                |
|  3. Client sends username + password                             |
|  4. Kafka verifies against JAAS config                           |
|  5. Connection established                                        |
+------------------------------------------------------------------+
```

## Connecting from Code

### Node.js (KafkaJS)
```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka:9092'],
  sasl: {
    mechanism: 'plain',
    username: 'backend',
    password: 'backend-secret',
  },
  ssl: false,  // true if using SASL_SSL
});
```

### Python (kafka-python)
```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='backend',
    sasl_plain_password='backend-secret'
)
```

### Java (Kafka Client)
```java
Properties props = new Properties();
props.put("bootstrap.servers", "kafka:9092");
props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config",
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"backend\" " +
    "password=\"backend-secret\";");
```

## Testing Authentication

### Test from Backend Container
```bash
# Enter backend container
docker exec -it lab-backend sh

# Test connection (should succeed with correct credentials)
# The backend will automatically authenticate using env variables
```

### Test from Host Machine
```bash
# Using kafka-console-producer with auth
docker exec -it lab-kafka kafka-console-producer \
  --broker-list localhost:9093 \
  --topic test \
  --producer-property security.protocol=SASL_PLAINTEXT \
  --producer-property sasl.mechanism=PLAIN \
  --producer-property sasl.jaas.config='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";'
```

## Files

- `kafka_server_jaas.conf` - Server-side JAAS configuration (defines all users)
- `kafka_jaas.conf` - Client-side JAAS configuration (for backend service)

## Security Notes

**For Production:**
1. Use SASL_SSL instead of SASL_PLAINTEXT
2. Use SASL/SCRAM instead of SASL/PLAIN (more secure)
3. Use strong, unique passwords
4. Enable ACLs (Access Control Lists) to restrict permissions
5. Rotate credentials regularly

**Current Setup (Development):**
- Protocol: SASL_PLAINTEXT (no SSL)
- Mechanism: PLAIN (simple username/password)
- No ACLs (all users have full access)

## Changing Passwords

1. Update `kafka_server_jaas.conf`:
   ```
   user_<username>="<new-password>"
   ```

2. Update `docker-compose.yml` environment variables

3. Update `backend/.env`

4. Restart services:
   ```bash
   docker-compose restart kafka backend kafka-ui
   ```
