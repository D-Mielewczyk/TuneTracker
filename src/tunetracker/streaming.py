"""
TuneTracker Streaming - Simple streaming with KafkaConsumer and PySpark
"""

import json
import os
import shutil
import signal
import sys
import time
import traceback
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StringType, StructType, TimestampType


def cleanup_spark_warehouse():
    """Clean up old files in spark-warehouse."""
    spark_warehouse_dir = "/app/spark-warehouse"
    if os.path.exists(spark_warehouse_dir):
        try:
            # Remove files older than 1 hour
            current_time = time.time()
            for item in os.listdir(spark_warehouse_dir):
                item_path = os.path.join(spark_warehouse_dir, item)
                if os.path.isfile(item_path) or os.path.isdir(item_path):
                    if current_time - os.path.getmtime(item_path) > 3600:  # 1 hour
                        if os.path.isdir(item_path):
                            shutil.rmtree(item_path)
                        else:
                            os.remove(item_path)
            logger.info("Cleaned up old spark-warehouse files")
        except Exception as e:
            logger.warning(f"Failed to cleanup spark-warehouse directory: {e}")


def consolidate_parquet_files(
    spark: SparkSession, output_path: str, target_file_size_gb: float = 1.0
) -> None:
    """
    Consolidate parquet files intelligently, considering existing files and aiming for target file size.

    Args:
        spark: SparkSession instance
        output_path: Path to parquet files to consolidate
        target_file_size_gb: Target file size in GB (default: 1.0)
    """
    logger.info(f"Starting intelligent parquet consolidation for {output_path}")

    # Clean up old spark-warehouse files first
    cleanup_spark_warehouse()

    try:
        # Check if output path exists and has parquet files
        path = Path(output_path)
        if not path.exists():
            logger.warning(
                f"Output path {output_path} does not exist, skipping consolidation"
            )
            return

        # Check if directory is being written to (has active processes)
        parquet_files = list(path.glob("*.parquet"))
        if not parquet_files:
            logger.info("No parquet files found, skipping consolidation")
            return

        # Wait a moment to ensure no active writes
        time.sleep(1)

        # Read all parquet files from the directory
        try:
            df = spark.read.parquet(output_path)
        except Exception as e:
            logger.warning(f"Cannot read parquet files from {output_path}: {e}")
            return

        # Get total record count
        total_records = df.count()
        if total_records == 0:
            logger.info("No records to consolidate")
            return

        # Get actual file sizes from the filesystem
        total_size_bytes = 0
        parquet_files = list(path.glob("*.parquet"))

        if parquet_files:
            # Calculate actual total size from existing parquet files
            for file_path in parquet_files:
                total_size_bytes += file_path.stat().st_size
            logger.info(f"Found {len(parquet_files)} existing parquet files")
        else:
            # Fallback to estimation if no existing files
            estimated_record_size = 200  # Default estimate in bytes
            total_size_bytes = total_records * estimated_record_size
            logger.info("No existing parquet files found, using size estimation")

        total_size_gb = total_size_bytes / (1024**3)

        logger.info(f"Total records: {total_records:,}")
        logger.info(f"Total size: {total_size_gb:.2f} GB")

        # Calculate target number of partitions for target file size
        target_partitions = max(1, int(total_size_gb / target_file_size_gb))

        # Ensure reasonable partition limits
        min_partitions = 1
        max_partitions = max(
            1, total_records // 1000000
        )  # At least 1M records per file

        target_partitions = max(min_partitions, min(target_partitions, max_partitions))

        logger.info(
            f"Target partitions for ~{target_file_size_gb}GB files: {target_partitions}"
        )

        # Repartition the dataframe
        consolidated_df = df.repartition(target_partitions)

        # Create backup of original files
        backup_path = f"{output_path}_backup_{int(time.time())}"
        try:
            # Copy original files to backup
            df.write.format("parquet").mode("overwrite").save(backup_path)
            logger.info(f"Backup created at: {backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")
            # Continue without backup if it fails

        # Write consolidated files to spark-warehouse directory
        spark_warehouse_dir = "/app/spark-warehouse"
        os.makedirs(spark_warehouse_dir, exist_ok=True)

        consolidated_output_path = (
            f"{spark_warehouse_dir}/consolidated_{int(time.time())}"
        )

        try:
            consolidated_df.write.format("parquet").mode("overwrite").save(
                consolidated_output_path
            )
            logger.success(f"Consolidated files written to: {consolidated_output_path}")

            # Clear the original demo_output directory after successful consolidation
            try:
                shutil.rmtree(output_path)
                logger.info(f"Cleared original directory: {output_path}")
            except Exception as clear_error:
                logger.warning(f"Could not clear original directory: {clear_error}")

            logger.info(f"Consolidated files available in: {consolidated_output_path}")

        except Exception as e:
            logger.error(f"Failed to write consolidated files: {e}")
            return

        # Verify the consolidation
        try:
            final_df = spark.read.parquet(consolidated_output_path)
            final_partitions = final_df.rdd.getNumPartitions()
            final_count = final_df.count()

            # Calculate final file sizes
            final_files = list(Path(consolidated_output_path).glob("*.parquet"))
            final_total_size = (
                sum(f.stat().st_size for f in final_files) if final_files else 0
            )
            final_total_size_gb = final_total_size / (1024**3)

            logger.success(
                f"✅ Consolidation completed successfully!\n"
                f"   Original records: {total_records:,}\n"
                f"   Final records: {final_count:,}\n"
                f"   Final partitions: {final_partitions}\n"
                f"   Final total size: {final_total_size_gb:.2f} GB\n"
                f"   Average file size: {final_total_size_gb / final_partitions:.2f} GB per file"
            )

            # Show sample of consolidated data
            logger.info("Sample of consolidated data:")
            final_df.show(5)

        except Exception as e:
            logger.error(f"Failed to verify consolidation: {e}")

    except Exception as e:
        logger.error(f"Consolidation failed: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")


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

    # Write results as parquet
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
