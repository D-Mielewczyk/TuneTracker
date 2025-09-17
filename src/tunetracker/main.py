import threading
import time

import typer
from kafka import KafkaConsumer
from loguru import logger

from .producer import run_producer
from .streaming import run_streaming, run_streaming_simple

app = typer.Typer()


@app.command()
def produce(
    bootstrap_servers: str = typer.Option(
        "localhost:9092", help="Kafka bootstrap servers"
    ),
    topic: str = typer.Option("music-plays", help="Kafka topic name"),
    rate: int = typer.Option(1, help="Messages per second"),
    total: int = typer.Option(100, help="Total messages to send"),
):
    """Produce random music play events to Kafka."""
    run_producer(
        bootstrap_servers=bootstrap_servers, topic=topic, rate=rate, total=total
    )


@app.command()
def stream(
    bootstrap_servers: str = typer.Option(
        "localhost:9092", help="Kafka bootstrap servers"
    ),
    input_topic: str = typer.Option("music-plays", help="Kafka input topic"),
    output_path: str = typer.Option("./output", help="Output path for results"),
    checkpoint_location: str = typer.Option("./checkpoint", help="Checkpoint location"),
):
    """Run the PySpark streaming aggregation job."""
    run_streaming(
        bootstrap_servers=bootstrap_servers,
        input_topic=input_topic,
        output_path=output_path,
        checkpoint_location=checkpoint_location,
    )


@app.command()
def demo(
    bootstrap_servers: str = typer.Option(
        "localhost:9092", help="Kafka bootstrap servers"
    ),
    topic: str = typer.Option("music-plays", help="Kafka topic name"),
    duration: int = typer.Option(60, help="Demo duration in seconds"),
):
    """Run a complete demo: produce events and stream them."""
    logger.info(f"Starting TuneTracker demo for {duration} seconds")

    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")

    max_retries = 30
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Try to create a consumer to test connection
            test_consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=1000,
            )
            test_consumer.close()
            logger.success("✅ Kafka is ready!")
            break
        except Exception as e:
            retry_count += 1
            logger.info(
                f"Exception: {e}. Waiting for Kafka... ({retry_count}/{max_retries})"
            )
            time.sleep(2)

    if retry_count >= max_retries:
        logger.error("❌ Kafka is not available after maximum retries")
        return

    # Start producer in background thread
    def run_producer_thread():
        run_producer(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            rate=1,
            total=None,
            duration=duration,
        )

    # Start streaming job in background thread (without signal handling)
    def run_streaming_thread():
        run_streaming_simple(
            bootstrap_servers=bootstrap_servers,
            input_topic=topic,
            output_path="./demo_output",
            checkpoint_location="./demo_checkpoint",
            duration=duration,
        )

    # Run both in separate threads
    producer_thread = threading.Thread(target=run_producer_thread)
    streaming_thread = threading.Thread(target=run_streaming_thread)

    producer_thread.start()
    streaming_thread.start()

    try:
        producer_thread.join()
        streaming_thread.join()

        logger.success("Demo completed!")

    except KeyboardInterrupt:
        logger.warning("Demo interrupted by user")


if __name__ == "__main__":
    app()
