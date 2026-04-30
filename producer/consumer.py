import json
import os
import time
import signal
import sys
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv
from config import logger

load_dotenv()

def shutdown(signum, frame):
    logger.info("Shutting down consumer...")
    try:
        consumer.close()
    except Exception:
        pass
    try:
        cursor.close()
        conn.close()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


def connect_postgres(retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "stock_data"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD"),
            )
            logger.info("✅ Connected to PostgreSQL successfully")
            return conn
        except Exception as e:
            logger.warning(f"PostgreSQL attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    logger.error("❌ All PostgreSQL connection attempts failed. Exiting.")
    sys.exit(1)


def connect_kafka(retries=5, delay=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                os.getenv("KAFKA_TOPIC", "stock_topic"),
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="stock-consumer-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            logger.info("✅ Kafka consumer started successfully")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    logger.error("❌ All Kafka connection attempts failed. Exiting.")
    sys.exit(1)


conn = connect_postgres()
cursor = conn.cursor()
consumer = connect_kafka()

logger.info("👂 Listening for messages...")

for message in consumer:
    try:
        data = message.value
        logger.info(f"📨 Received: {data.get('symbol')} @ {data.get('date')}")

        cursor.execute(
            """
            INSERT INTO stock_data (date, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                data.get("date"),
                data.get("symbol"),
                data.get("open"),
                data.get("high"),
                data.get("low"),
                data.get("close"),
                data.get("volume"),
            ),
        )
        conn.commit()
        logger.info(f"✅ Inserted: {data.get('symbol')} @ {data.get('date')}")

    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")
        conn.rollback()