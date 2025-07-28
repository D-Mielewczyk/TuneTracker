#!/usr/bin/env python3
"""
Simple Kafka connectivity test using kafka-python
"""

import json
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from loguru import logger


def test_kafka_connection(bootstrap_servers="localhost:9092"):
    """Test Kafka connectivity and basic operations."""
    logger.info(f"Testing Kafka connection to {bootstrap_servers}")

    try:
        # Test producer
        logger.info("Creating KafkaProducer...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            request_timeout_ms=5000,
        )
        logger.success("✅ KafkaProducer created successfully")

        # Test sending a message
        test_topic = "test-topic"
        test_message = {"test": "message", "timestamp": time.time()}

        logger.info(f"Sending test message to topic '{test_topic}'...")
        future = producer.send(test_topic, value=test_message)

        # Wait for the message to be sent
        record_metadata = future.get(timeout=10)
        logger.info(
            f"✅ Message sent successfully to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}"
        )

        # Test consumer
        logger.info("Creating KafkaConsumer...")
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )
        logger.success("✅ KafkaConsumer created successfully")

        # Try to read the message
        logger.info("Reading message from consumer...")
        message = next(consumer, None)
        if message:
            logger.success(f"✅ Message received: {message.value}")
        else:
            logger.warning(
                "⚠️ No message received (this might be normal if topic was empty)"
            )

        producer.close()
        consumer.close()
        logger.success("✅ Kafka test completed successfully!")
        return True

    except NoBrokersAvailable:
        logger.error("❌ No Kafka brokers available. Is Kafka running?")
        logger.warning("Try starting Kafka with: docker-compose up -d")
        return False
    except KafkaError as e:
        logger.error(f"❌ Kafka error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    test_kafka_connection()
