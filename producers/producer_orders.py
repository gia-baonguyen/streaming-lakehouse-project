import json
import time
import random
from kafka import KafkaProducer
# Import datetime (or also timezone if using the other method)
from datetime import datetime #, timezone

# Connect to ports mapped to the host
KAFKA_BROKERS = 'localhost:29092,localhost:39092,localhost:49092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize dictionary to JSON bytes
    client_id='orders-producer' # Identifier for this producer
)

topic_name = 'orders_topic' # Target Kafka topic
order_id_counter = 1 # Simple counter for unique order IDs

print(f"Starting producer for topic: {topic_name} targeting brokers: {KAFKA_BROKERS}")
try:
    while True:
        # Generate random order data
        price = round(random.uniform(10.0, 500.0), 2)
        quantity = random.randint(1, 5)
        order_data = {
            'order_id': f'ORD-{order_id_counter:05d}',
            'user_id': f'user_{random.randint(1, 100)}', # Simulate random user
            'product_id': f'prod_{random.randint(1, 50)}', # Simulate random product
            'quantity': quantity,
            'price': price, # Add price
            # Generate timestamp in UTC ISO format
            'order_time': datetime.utcnow().isoformat(), # <-- Changed to utcnow() for consistency
            # Alternative using timezone-aware UTC:
            # 'order_time': datetime.now(timezone.utc).isoformat(),
            'status': random.choice(['CREATED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED']) # Example statuses
        }
        # Print order being sent (for debugging/visibility)
        print(f"Sending Order: {order_data}")

        # Send the message to the Kafka topic
        producer.send(topic_name, value=order_data)

        # Ensure message is sent immediately (useful for low volume, can impact high throughput)
        producer.flush()

        order_id_counter += 1 # Increment counter

        # Wait for a random interval (keep commented out for load testing)
        # time.sleep(random.uniform(0.5, 2.0))
        # Add a small sleep if needed to control rate when the longer sleep is off
        time.sleep(0.2) # Example: Send roughly 10 orders/sec

except KeyboardInterrupt:
    # Handle graceful shutdown on Ctrl+C
    print("Orders Producer stopped by user.")
except Exception as e:
    # Catch and print any Kafka connection or sending errors
    print(f"Error connecting or sending to Kafka: {e}")
finally:
    # Ensure the producer connection is closed cleanly
    print("Closing Kafka producer.")
    producer.close()