import pandas as pd
import json
import time
from kafka import KafkaProducer
import logging
import sys

# Configure logging for better visibility
logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# --- Configuration ---
# Set the Kafka broker address (where the server is running)
# In the Colab setup, Kafka runs on localhost:9092
KAFKA_BROKER = 'localhost:9092'
# Define the name of the topic to send data to
KAFKA_TOPIC = 'terai_weather_stream'
# Define the local CSV file to read
CSV_FILE_PATH = './simulated_weather_data.csv' 
# Time delay between sending messages (in seconds)
STREAM_INTERVAL_SECONDS = 0.5 

def json_serializer(data):
    """Custom serializer to encode Python dictionary (row data) to JSON bytes."""
    return json.dumps(data).encode('utf-8')

def create_topic_and_producer():
    """Initializes the Kafka Producer and ensures the topic exists (if broker is configured to auto-create)."""
    try:
        # Initialize the Kafka Producer
        # value_serializer ensures the message data is correctly formatted
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer,
            # Let kafka-python auto-detect the API version
            api_version=None,
            request_timeout_ms=10000,
            # Set a high buffer size to handle bursty writes
            max_block_ms=20000
        )
        logger.info(f"Successfully connected to Kafka broker: {KAFKA_BROKER}")

        # Note: In the standard Colab setup, the topic must be created manually 
        # or rely on Kafka's auto-topic creation (which is usually enabled).
        logger.info(f"Targeting Kafka Topic: {KAFKA_TOPIC}")
        return producer
    except Exception as e:
        logger.error(f"Error connecting to Kafka broker or initializing producer: {e}")
        # Exit if the producer cannot be initialized
        sys.exit(1)

def stream_csv_data(producer):
    """Reads a CSV file line by line and streams the data to Kafka."""
    try:
        # Load the CSV file using pandas
        df = pd.read_csv(CSV_FILE_PATH)
        logger.info(f"Successfully loaded {len(df)} records from {CSV_FILE_PATH}")
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at '{CSV_FILE_PATH}'. Please upload it to Colab.")
        return
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return

    logger.info("Starting data stream...")
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Convert the pandas series (row) to a Python dictionary
        data = row.to_dict()
        
        # Send the data dictionary to Kafka
        try:
            future = producer.send(KAFKA_TOPIC, value=data)
            
            # Optional: Wait for confirmation (useful for debugging, uncomment if needed)
            # record_metadata = future.get(timeout=5)
            # logger.info(f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")

            logger.info(f"Sent message {index + 1}/{len(df)}")
            
            # Introduce a time delay to simulate a continuous stream
            time.sleep(STREAM_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Failed to send message {index + 1}: {e}")
            # Continue to the next message if one fails
            pass 

    # Ensure all buffered messages are sent before closing
    producer.flush()
    logger.info("Data streaming complete. All messages flushed.")


if __name__ == "__main__":
    # 1. Create the Producer connection
    producer = create_topic_and_producer()
    
    # 2. Before streaming, ensure the topic is created 
    # (If auto-create is disabled, run the manual topic creation command 
    # in the Colab terminal:
    # !kafka_2.13-3.7.0/bin/kafka-topics.sh --create --topic realtime_data_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    
    # 3. Start the data stream
    stream_csv_data(producer)