import time
from config import logger
from extract import connect_to_api, extract_jason
from producer_setup import kafka_producer, topic_name


def main() -> None:
    """
    Orchestrates stock data collection and streaming to Kafka:
    1. Fetches stocks from API using 'connect_to_api()'.
    2. Extracts and formats data using 'extract_jason()'.
    3. Sends processed data to Kafka topic via 'kafka_producer()'.
    4. Introduces a 2-second delay between each record.
    """
    response = connect_to_api()
    data = extract_jason(response)
    producer = kafka_producer()

    for stock in data:
        result = {
            "date": stock["date"],
            "symbol": stock["symbol"],
            "open": stock["open"],
            "high": stock["high"],
            "low": stock["low"],
            "close": stock["close"],
            "volume": stock["volume"],
        }
        producer.send(topic_name, value=result)
        logger.info(f"Sent stock data for {stock['symbol']} to Kafka topic '{topic_name}'")
        time.sleep(2)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()