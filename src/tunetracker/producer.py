import json
import random
import time
from datetime import datetime
from typing import Any, Dict

from kafka import KafkaProducer
from loguru import logger

# Constants
GENRES = [
    "pop",
    "rock",
    "jazz",
    "classical",
    "electronic",
    "hip-hop",
    "country",
    "blues",
]


def generate_random_message() -> Dict[str, Any]:
    """Generate a random music play event."""
    return {
        "user_id": f"u{random.randint(1000, 9999)}",
        "track_id": f"t{random.randint(100000, 999999)}",
        "genre": random.choice(GENRES),
        "timestamp": datetime.now().isoformat(),
    }


def run_producer(
    bootstrap_servers: str, topic: str, rate: int, total: int, duration: int = None
) -> None:
    """Run the Kafka producer to send music play events.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic name
        rate: Messages per second
        total: Total messages to send (if duration is None)
        duration: Duration in seconds (if provided, overrides total)
    """
    logger.info(f"Starting producer: {rate} msg/sec")
    if duration:
        logger.info(f"Duration: {duration} seconds")
    else:
        logger.info(f"Total messages: {total}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            request_timeout_ms=5000,
        )
        logger.success("✅ KafkaProducer created successfully")
    except Exception as e:
        logger.error(f"❌ Error creating KafkaProducer: {e}")
        return

    try:
        if duration:
            # Run for specified duration
            start_time = time.time()
            message_count = 0

            while time.time() - start_time < duration:
                message = generate_random_message()
                producer.send(topic, value=message)
                message_count += 1
                logger.info(f"Sent message {message_count}: {message}")
                time.sleep(1.0 / rate)
        else:
            # Run for specified number of messages
            for i in range(total):
                message = generate_random_message()
                producer.send(topic, value=message)
                logger.info(f"Sent message {i + 1}/{total}: {message}")

                if i < total - 1:  # Don't sleep after the last message
                    time.sleep(1.0 / rate)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped")
