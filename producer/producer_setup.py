from kafka import KafkaProducer
import json
import os 

topic_name = os.getenv("KAFKA_TOPIC", "stock_data")

def kafka_producer() -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer instance for sending messages to a Kafka topic.

    This function creates a KafkaProducer object configured to connect to a Kafka broker
    running on localhost at port 9092. The producer is set up to serialize messages as JSON.

    Returns:
        KafkaProducer: An instance of KafkaProducer configured for JSON serialization.

    Example:(call the function to create a Kafka producer):
        producer = kafka_producer()
        producer.send(stock_data, value={"key": "value"})

    
    """
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),  # ✅ reads from .env 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer