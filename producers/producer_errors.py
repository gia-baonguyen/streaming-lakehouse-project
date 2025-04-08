import json
import time
import random
from kafka import KafkaProducer
# Import datetime (or also timezone if using the other method)
from datetime import datetime #, timezone

# Kafka broker list accessible from the host machine
KAFKA_BROKERS = 'localhost:29092,localhost:39092,localhost:49092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Serialize dictionary to JSON bytes
    client_id='errors-producer' # Identifier for this producer instance
)

topic_name = 'errors_topic' # Target Kafka topic
error_id_counter = 1 # Simple counter for unique error IDs

print(f"Starting producer for topic: {topic_name} targeting brokers: {KAFKA_BROKERS}")
try:
    while True:
        # Simulate infrequent errors
        if random.random() < 0.05: # 5% chance of error per cycle (adjust probability as needed)
            error_data = {
                'error_id': f'ERR-{error_id_counter:06d}',
                'service_name': random.choice(['payment-service', 'order-service', 'frontend-web', 'recommendation-engine']), # Example services
                'error_message': random.choice(['Database connection timeout', 'NullPointerException', 'Invalid user input', 'Service Unavailable', 'API rate limit exceeded']), # Example messages
                'severity': random.choice(['WARN', 'ERROR', 'FATAL']), # Random severity
                # Generate timestamp in UTC ISO format
                'error_time': datetime.utcnow().isoformat() # <-- Changed to utcnow() for consistency
                # Alternative using timezone-aware UTC:
                # 'error_time': datetime.now(timezone.utc).isoformat()
            }
            # Print the error being sent (for debugging/visibility)
            print(f"Sending Error: {error_data}")

            # Send the message to the Kafka topic
            producer.send(topic_name, value=error_data)

            # Ensure messages are sent immediately
            producer.flush()

            error_id_counter += 1 # Increment counter

        # Add a small sleep even if no error is generated to prevent tight loop
        # when the main sleep inside 'if' is commented out for load testing.
        time.sleep(0.2) # Adjust sleep time as needed (e.g., 0.1 for faster checks)

        # Original longer sleep (if error occurs) - Keep commented out for load testing
        # if random.random() < 0.05:
        #    ... send error ...
        #    # time.sleep(random.uniform(1.0, 3.0)) # Original location of sleep

except KeyboardInterrupt:
    # Handle graceful shutdown on Ctrl+C
    print("Errors Producer stopped by user.")
except Exception as e:
    # Catch and print any Kafka connection or sending errors
    print(f"Error connecting or sending to Kafka: {e}")
finally:
    # Ensure the producer connection is closed cleanly
    print("Closing Kafka producer.")
    producer.close()