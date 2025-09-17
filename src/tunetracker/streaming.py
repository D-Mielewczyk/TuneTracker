import csv
import json
import os
import signal
import sys
import time
import traceback

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, window
from pyspark.sql.types import StringType, StructType, TimestampType


def create_spark_session():
    """Create SparkSession optimized for Docker environment."""
    logger.info("Creating SparkSession for Docker environment...")

    try:
        # Create SparkSession with Docker-optimized settings
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("TuneTrackerStreaming")
            .config("spark.driver.host", "0.0.0.0")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.sql.warehouse.dir", "/app/spark-warehouse")
            .config("spark.sql.streaming.checkpointLocation", "/app/checkpoint")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.sql.adaptive.skewJoin.enabled", "false")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "false")
            .config("spark.local.dir", "/app/spark-warehouse")
            .getOrCreate()
        )

        logger.success("✅ SparkSession created successfully in Docker!")
        return spark
    except Exception as e:
        logger.error(f"❌ Failed to create SparkSession: {e}")
        return None


def test_spark_session():
    """Test if we can create a minimal SparkSession."""
    logger.info("Testing minimal SparkSession creation...")
    try:
        # Create a simple SparkSession
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("TestSpark")
            .config("spark.driver.host", "0.0.0.0")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .getOrCreate()
        )

        logger.success("✅ Minimal SparkSession created successfully!")
        spark.stop()
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create minimal SparkSession: {e}")
        return False


def process_messages_batch(messages, spark, output_path):
    """Process a batch of messages and append results to a single CSV."""
    # Convert messages to Spark DataFrame
    schema = (
        StructType()
        .add("user_id", StringType())
        .add("track_id", StringType())
        .add("genre", StringType())
        .add("timestamp", StringType())
    )

    # Create DataFrame from collected messages
    df = spark.createDataFrame(messages, schema=schema)
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    # Aggregate by window and genre
    agg_df = df.groupBy(window(col("timestamp"), "1 minute"), col("genre")).count()

    # Flatten window struct and format timestamps for CSV
    csv_df = agg_df.select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("genre"),
        col("count"),
    )

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    single_csv = os.path.join(output_path, "results.csv")

    # Write header once, then append rows
    write_header = not os.path.exists(single_csv)

    # Append rows without collecting all to driver memory at once
    with open(single_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["window_start", "window_end", "genre", "count"])
        for row in csv_df.toLocalIterator():
            writer.writerow(
                [row["window_start"], row["window_end"], row["genre"], row["count"]]
            )

    logger.success(f"✅ Appended batch to {single_csv}")
    logger.info("Sample results:")
    csv_df.show()


def shutdown_hook(spark_session: SparkSession):
    """Graceful shutdown handler for Spark session."""

    def handler(signum, frame):
        logger.info("Graceful shutdown initiated...")
        spark_session.stop()
        sys.exit(0)

    return handler


def run_streaming_simple(
    bootstrap_servers: str,
    input_topic: str,
    output_path: str,
    checkpoint_location: str = "./checkpoint",
    duration: int = 120,
) -> None:
    """Run streaming without signal handling (for threaded execution).

    Args:
        bootstrap_servers: Kafka bootstrap servers
        input_topic: Kafka input topic
        output_path: Output path for results
        checkpoint_location: Checkpoint location for Spark streaming
        duration: Duration to run the streaming job in seconds (default: 120)
    """

    logger.info(f"Starting streaming job: {input_topic} -> {output_path}")

    # Test Spark session
    test_spark_session()

    # Create directories
    os.makedirs(checkpoint_location, exist_ok=True)
    logger.info("Checkpoint directory created/verified")

    os.makedirs("./spark-warehouse", exist_ok=True)
    logger.info("Spark-warehouse directory created/verified")

    # Create SparkSession
    logger.info("Creating SparkSession...")
    spark = create_spark_session()
    if not spark:
        logger.error("Failed to create SparkSession")
        return

    logger.info("SparkSession created successfully")

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,  # 1 second timeout
        )
        logger.success("✅ KafkaConsumer created successfully")
    except NoBrokersAvailable:
        logger.error("❌ No Kafka brokers available. Is Kafka running?")
        logger.warning("Try starting Kafka with: docker-compose up -d")
        return
    except Exception as e:
        logger.error(f"❌ Error creating KafkaConsumer: {e}")
        return

    # Collect messages from Kafka continuously
    messages = []
    logger.info("Collecting messages from Kafka...")
    batch_count = 0
    last_save_time = time.time()
    save_interval = 60  # Save every 60 seconds

    start_time = time.time()

    try:
        while time.time() - start_time < duration:
            try:
                # Use a shorter timeout to check for new messages
                message = next(consumer, None)
                if message is None:
                    # No message available, continue the loop
                    time.sleep(0.1)
                    continue

                messages.append(message.value)
                logger.info(f"Received message: {message.value}")

                current_time = time.time()

                # Save every 60 seconds or when we have enough messages
                if (current_time - last_save_time >= save_interval and messages) or len(
                    messages
                ) >= 10:
                    batch_count += 1
                    logger.info(
                        f"Processing batch {batch_count} with {len(messages)} messages..."
                    )

                    # Process current batch
                    process_messages_batch(messages, spark, output_path)

                    # Clear messages for next batch
                    messages = []
                    last_save_time = current_time

            except StopIteration:
                # No more messages available, continue the loop
                time.sleep(0.1)
                continue

    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        consumer.close()
        return

    consumer.close()

    # Process any remaining messages at the end
    if messages:
        batch_count += 1
        logger.info(
            f"Processing final batch {batch_count} with {len(messages)} messages..."
        )
        process_messages_batch(messages, spark, output_path)
    elif batch_count == 0:
        logger.warning("No messages received from Kafka")
        return

    logger.info(f"Demo completed! Processed {batch_count} batches total.")

    spark.stop()


def run_streaming(
    bootstrap_servers: str,
    input_topic: str,
    output_path: str,
    checkpoint_location: str = "./checkpoint",
) -> None:
    """Run the streaming aggregation job using KafkaConsumer and PySpark.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        input_topic: Kafka input topic
        output_path: Output path for results
        output_format: Output format (csv or parquet)
        checkpoint_location: Checkpoint location for Spark streaming
    """
    logger.info(f"Starting streaming job: {input_topic} -> {output_path}")

    # Test SparkSession creation first
    if not test_spark_session():
        logger.error("Cannot proceed - SparkSession creation failed")
        return

    # Create checkpoint directory if it doesn't exist
    logger.info(f"Creating checkpoint directory: {checkpoint_location}")
    os.makedirs(checkpoint_location, exist_ok=True)
    logger.info("Checkpoint directory created/verified")

    # Create spark-warehouse directory
    logger.info("Creating spark-warehouse directory...")
    os.makedirs("/app/spark-warehouse", exist_ok=True)
    logger.info("Spark-warehouse directory created/verified")

    try:
        logger.info("Creating SparkSession...")

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("TuneTrackerStreaming")
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
            .config("spark.driver.host", "0.0.0.0")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.sql.warehouse.dir", "/app/spark-warehouse")
            .config("spark.local.dir", "/app/spark-warehouse")
            .getOrCreate()
        )
        logger.info("SparkSession created successfully")

    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        logger.error(f"Exception type: {type(e).__name__}")

        logger.error(f"Full traceback: {traceback.format_exc()}")
        return

    # Set up graceful shutdown
    signal.signal(signal.SIGINT, shutdown_hook(spark))
    signal.signal(signal.SIGTERM, shutdown_hook(spark))

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        logger.success("✅ KafkaConsumer created successfully")
    except NoBrokersAvailable:
        logger.error("❌ No Kafka brokers available. Is Kafka running?")
        logger.warning("Try starting Kafka with: docker-compose up -d")
        return
    except Exception as e:
        logger.error(f"❌ Error creating KafkaConsumer: {e}")
        return

    # Collect messages from Kafka continuously
    messages = []
    logger.info("Collecting messages from Kafka...")
    batch_count = 0

    try:
        for message in consumer:
            messages.append(message.value)
            logger.info(f"Received message: {message.value}")

            # Process in batches of 10 messages
            if len(messages) >= 10:
                batch_count += 1
                logger.info(
                    f"Processing batch {batch_count} with {len(messages)} messages..."
                )

                # Process current batch
                process_messages_batch(messages, spark, output_path)

                # Clear messages for next batch
                messages = []

                # Continue collecting for more batches
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        consumer.close()
        return

    consumer.close()

    # Process any remaining messages
    if messages:
        logger.info(f"Processing final batch with {len(messages)} messages...")
        process_messages_batch(messages, spark, output_path)
    elif batch_count == 0:
        logger.warning("No messages received from Kafka")
        return

    spark.stop()
