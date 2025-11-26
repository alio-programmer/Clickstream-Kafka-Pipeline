import json 
import random
import uuid
import time
from datetime import datetime, date, timezone, timedelta
import threading
from kafka import KafkaProducer, KafkaConsumer

TOPIC = "clickstream_events"
KAFKA_BROKERS = '127.0.0.1:9092' 
CONSUMER = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='clickstream-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def kafka_consumer():
    """
    Consumes data from Kafka and handles graceful shutdown.
    """
    print(f"Starting consumer on topic: {TOPIC}... press Ctrl+C to stop.")
    try:
        #wait for messages in the topic 
        for message in CONSUMER:
            print(f"Consumed event: {message.value}")
    
    except KeyboardInterrupt:
        print("\nInterrupt detected. Closing consumer...")

    except Exception as e:
        print(f"Error in consumer: {e}")
    
    finally:
        CONSUMER.close()
        print("Consumer closed successfully.")

if __name__ == "__main__":
    kafka_consumer()