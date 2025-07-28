"""
TuneTracker Streaming - Simple streaming with KafkaConsumer and PySpark
"""

import json
import signal
import sys
import time
import os
import csv
from datetime import datetime
from typing import Optional
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
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


def shutdown_hook(spark_session: SparkSession):
    """Graceful shutdown handler for Spark session."""

    def handler(signum, frame):
        logger.info("Graceful shutdown initiated...")
        spark_session.stop()
        sys.exit(0)

    return handler


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
        output_format: Output format (csv or delta)
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
        import traceback

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

    # Collect messages from Kafka
    messages = []
    logger.info("Collecting messages from Kafka...")

    try:
        for message in consumer:
            messages.append(message.value)
            logger.info(f"Received message: {message.value}")

            # Process in batches of 10 messages
            if len(messages) >= 10:
                break
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        consumer.close()
        return

    consumer.close()

    if not messages:
        logger.warning("No messages received from Kafka")
        return

    logger.info(f"Processing {len(messages)} messages...")

    # Try Spark processing first, fallback to pandas if it fails
    spark = create_spark_session()
    if spark:
        # Process with Spark
        logger.info("Using Spark processing...")

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
            agg_df.write.format("csv").option("header", True).save(output_path)
        else:
            agg_df.write.format("delta").save(output_path)

        logger.success(f"✅ Results written to {output_path}")
        logger.info("Sample results:")
        agg_df.show()

        spark.stop()
    else:
        # Fallback to pandas processing
        logger.info("Using pandas fallback processing...")
        import pandas as pd
        from datetime import datetime

        # Convert messages to pandas DataFrame
        df = pd.DataFrame(messages)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Aggregate by genre (simplified without windowing)
        genre_counts = df.groupby("genre").size().reset_index(name="count")

        # Write results
        output_file = os.path.join(output_path, "genre_counts.csv")
        os.makedirs(output_path, exist_ok=True)
        genre_counts.to_csv(output_file, index=False)

        logger.success(f"✅ Results written to {output_file}")
        logger.info("Sample results:")
        logger.info(genre_counts.to_string())
