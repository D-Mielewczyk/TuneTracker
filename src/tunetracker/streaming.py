"""
TuneTracker Streaming - Simple streaming with KafkaConsumer and PySpark
"""

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
from pyspark.sql.functions import col, window
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


def process_messages_batch(messages, spark, output_path, output_format):
    """Process a batch of messages and save results."""
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

    # Write results
    if output_format == "csv":
        # For CSV, we need to flatten the window struct
        agg_df = agg_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("genre"),
            col("count"),
        )
        agg_df.write.format("csv").option("header", True).mode("append").save(
            output_path
        )
    else:
        # For Parquet (columnar format), we can keep the window struct
        agg_df.write.format("parquet").mode("append").save(output_path)

    logger.success(f"✅ Batch results written to {output_path}")
    logger.info("Sample results:")
    agg_df.show()


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
    output_format: str = "csv",
    checkpoint_location: str = "./checkpoint",
) -> None:
    """Run streaming without signal handling (for threaded execution)."""

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
    duration = 120  # Match the demo duration

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
                    process_messages_batch(messages, spark, output_path, output_format)

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
        process_messages_batch(messages, spark, output_path, output_format)
    elif batch_count == 0:
        logger.warning("No messages received from Kafka")
        return

    logger.info(f"Demo completed! Processed {batch_count} batches total.")

    # Consolidate Parquet files before ending
    if output_format == "parquet":
        logger.info("Consolidating Parquet files...")
        try:
            # Read all parquet files and repartition to consolidate
            consolidated_df = spark.read.parquet(output_path)

            # Count total records for logging
            total_records = consolidated_df.count()
            logger.info(f"Total records to consolidate: {total_records}")

            # Repartition to create larger files (aim for ~1GB files)
            # Assuming average record size of ~200 bytes, target ~5M records per file
            target_partitions = max(1, total_records // 5000000)
            consolidated_df = consolidated_df.repartition(target_partitions)

            # Write consolidated files
            consolidated_df.write.format("parquet").mode("overwrite").save(
                f"{output_path}_consolidated"
            )

            # Count final files
            final_files = spark.read.parquet(
                f"{output_path}_consolidated"
            ).rdd.getNumPartitions()
            logger.success(
                f"✅ Consolidated {total_records} records into {final_files} files"
            )

        except Exception as e:
            logger.warning(f"Consolidation failed: {e}")

    spark.stop()


def run_streaming(
    bootstrap_servers: str,
    input_topic: str,
    output_path: str,
    output_format: str = "csv",
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
                process_messages_batch(messages, spark, output_path, output_format)

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
        process_messages_batch(messages, spark, output_path, output_format)
    elif batch_count == 0:
        logger.warning("No messages received from Kafka")
        return

    spark.stop()
