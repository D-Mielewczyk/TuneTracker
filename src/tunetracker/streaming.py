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


def test_spark_session():
    """Test if we can create a minimal SparkSession."""
    logger.info("Testing minimal SparkSession creation...")
    try:
        # Try a completely different approach - use PySpark's built-in Spark only
        logger.info("Trying with PySpark's built-in Spark only...")

        # Remove all external Spark environment variables
        for key in [
            "SPARK_HOME",
            "SPARK_LOCAL_IP",
            "SPARK_LOCAL_HOSTNAME",
            "SPARK_DRIVER_OPTS",
            "SPARK_EXECUTOR_OPTS",
        ]:
            if key in os.environ:
                del os.environ[key]
                logger.info(f"Removed {key}")

        # Set only essential environment variables
        os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"

        # Try with explicit Java options
        import subprocess
        import sys

        # Check if we can find Java
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True, timeout=10
            )
            logger.info(f"Java version check: {result.returncode}")
        except Exception as e:
            logger.error(f"Java check failed: {e}")

        # Try using a different approach - import SparkContext first
        logger.info("Trying with SparkContext initialization...")
        from pyspark import SparkContext, SparkConf

        # Create a minimal SparkConf
        conf = SparkConf()
        conf.setMaster("local[1]")
        conf.setAppName("TestSpark")
        conf.set("spark.driver.host", "127.0.0.1")
        conf.set("spark.driver.bindAddress", "127.0.0.1")

        # Try to create SparkContext first
        logger.info("Creating SparkContext...")
        sc = SparkContext(conf=conf)
        logger.info("SparkContext created successfully!")

        # Now create SparkSession from the SparkContext
        logger.info("Creating SparkSession from SparkContext...")
        spark = SparkSession(sc)

        logger.success("✅ Minimal SparkSession created successfully!")
        spark.stop()
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create minimal SparkSession: {e}")
        logger.warning(
            "⚠️ SparkSession creation failed. Using fallback approach with pandas..."
        )
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

    # Debug environment variables
    logger.info("=== Environment Debug ===")
    logger.info(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'NOT SET')}")
    logger.info(f"SPARK_HOME: {os.environ.get('SPARK_HOME', 'NOT SET')}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Python executable: {sys.executable}")
    logger.info("=== End Environment Debug ===")

    # Set additional environment variables for Windows
    logger.info("Setting Windows-specific environment variables...")
    os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:\\spark")
    os.environ["SPARK_LOCAL_DIRS"] = os.path.abspath("./spark-temp")
    os.environ["SPARK_WORKER_DIR"] = os.path.abspath("./spark-worker")

    # Create temp directories
    os.makedirs("./spark-temp", exist_ok=True)
    os.makedirs("./spark-worker", exist_ok=True)
    logger.info("Environment variables and temp directories set")

    # Create checkpoint directory if it doesn't exist
    logger.info(f"Creating checkpoint directory: {checkpoint_location}")
    os.makedirs(checkpoint_location, exist_ok=True)
    logger.info("Checkpoint directory created/verified")

    # Create spark-warehouse directory for Windows
    logger.info("Creating spark-warehouse directory...")
    os.makedirs("./spark-warehouse", exist_ok=True)
    logger.info("Spark-warehouse directory created/verified")

    # Initialize Spark with Windows-specific configurations
    logger.info("Before SparkSession creation...")
    logger.info(f"Checkpoint location: {checkpoint_location}")

    try:
        logger.info("Creating SparkSession with Windows-specific configs...")

        # Try to import and check PySpark version
        import pyspark

        logger.info(f"PySpark version: {pyspark.__version__}")

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("TuneTrackerStreaming")
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location)
            .config("spark.hadoop.dfs.permissions", "false")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.sql.adaptive.enabled", "false")
            # Windows-specific configurations
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.sql.warehouse.dir", "./spark-warehouse")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.sql.adaptive.skewJoin.enabled", "false")
            .config("spark.sql.adaptive.localShuffleReader.enabled", "false")
            .getOrCreate()
        )
        logger.info("After SparkSession creation...")

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
    if test_spark_session():
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
