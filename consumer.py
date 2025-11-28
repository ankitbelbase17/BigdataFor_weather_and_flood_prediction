import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys

# --- Configuration ---
BOOTSTRAP_SERVERS = ['localhost:9092'] 
TOPIC_NAME = 'terai_weather_stream'
CONSUMER_GROUP = 'terai-flood-risk-group-1'
# ---------------------

def json_deserializer(m):
    """Deserialize JSON bytes to a Python dictionary."""
    try:
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def consume_data_stream():
    """Subscribes to the Kafka topic and processes incoming messages."""
    
    # 1. Initialize Consumer
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            group_id=CONSUMER_GROUP,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            # Start reading from the beginning of the topic if no committed offset is found
            auto_offset_reset='earliest',
            # Automatically commit offsets periodically
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=json_deserializer
        )
        print(f"[OK] Kafka Consumer initialized. Listening to topic: {TOPIC_NAME}")
        print("Waiting for messages from the producer...")
    except Exception as e:
        print(f"[ERROR] Failed to initialize Kafka Consumer. Ensure Kafka is running at {BOOTSTRAP_SERVERS}.")
        print(f"Error: {e}")
        sys.exit(1)
        
    # 2. Consume Messages
    try:
        for message in consumer:
            if message.value is None:
                print("Received non-JSON message or decoding failed. Skipping.")
                continue

            # The message.value is now the deserialized Python dictionary
            record = message.value
            
            # Example Processing/Triage: Check for high rainfall (Potential flood risk)
            if record.get('rainfall_mm_per_hr', 0) > 5.0:
                status = "[ALERT] FLOOD ALERT (High Rainfall!)"
            else:
                status = "Status OK"
                
            print("-" * 50)
            print(f"Topic: {message.topic} | Partition: {message.partition} | Offset: {message.offset}")
            print(f"TIME: {record.get('timestamp')}")
            print(f"DATA: Rainfall={record.get('rainfall_mm_per_hr')} mm/hr, River Level={record.get('river_level_m')} m")
            print(f"ACTION: {status}")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred during consumption: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_data_stream()