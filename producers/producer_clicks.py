import json
import time
import random
from kafka import KafkaProducer
# Import datetime (or also timezone if using the other method)
from datetime import datetime #, timezone

# Kafka broker list accessible from the host machine
KAFKA_BROKERS = 'localhost:29092,localhost:39092,localhost:49092'

# Initialize Kafka Producer
# Uses json serializer and specifies bootstrap servers
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize dictionary to JSON bytes
    client_id='clicks-producer' # Identifier for this producer instance
)

topic_name = 'clicks_topic' # Target Kafka topic
click_id_counter = 1 # Simple counter for unique click IDs

print(f"Starting producer for topic: {topic_name} targeting brokers: {KAFKA_BROKERS}")
try:
    while True:
        # Generate random click event data
        click_data = {
            'click_id': f'CLK-{click_id_counter:07d}',
            'user_id': f'user_{random.randint(1, 100)}', # Simulate random user
            'url': f'/page/{random.choice(["home", "product", "cart", "profile"])}', # Simulate random page URL
            'device_type': random.choice(['desktop', 'mobile', 'tablet']), # Simulate random device
            # Generate timestamp in UTC ISO format for consistency
            'click_time': datetime.utcnow().isoformat() # <-- Changed to utcnow() for consistency
            # Alternative using timezone-aware UTC:
            # 'click_time': datetime.now(timezone.utc).isoformat()
        }
        # Print message being sent (for debugging/visibility)
        print(f"Sending Click: {click_data}")

        # Send the message to the Kafka topic
        producer.send(topic_name, value=click_data)

        # Ensure messages are sent immediately (useful for low volume, might impact high throughput)
        producer.flush()

        click_id_counter += 1 # Increment counter

        # Wait for a random interval before sending the next message
        time.sleep(random.uniform(0.1, 1.0)) # Keep commented out for high-load simulation

except KeyboardInterrupt:
    # Handle graceful shutdown on Ctrl+C
    print("Clicks Producer stopped by user.")
except Exception as e:
    # Catch and print any Kafka connection or sending errors
    print(f"Error connecting or sending to Kafka: {e}")
finally:
    # Ensure the producer connection is closed cleanly
    print("Closing Kafka producer.")
    producer.close()