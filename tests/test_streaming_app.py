import os
import sys

# point findspark at your unpacked Spark and Hadoop homes
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

import findspark

findspark.init()  # adds C:\spark\python and its py4j zip to sys.path

import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StringType, StructType

# Import streaming module for testing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from tunetracker import streaming


def test_streaming_aggregation(tmp_path):
    # force Python worker to use the same interpreter
    python_exec = sys.executable

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("TestTuneTracker")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        # disable permission calls (so we never hit winutils or nativeIO)
        .config("spark.hadoop.dfs.permissions", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    data = [
        {
            "user_id": "u1",
            "track_id": "t1",
            "genre": "pop",
            "timestamp": "2024-01-01T00:00:00",
        },
        {
            "user_id": "u2",
            "track_id": "t2",
            "genre": "pop",
            "timestamp": "2024-01-01T00:00:10",
        },
        {
            "user_id": "u3",
            "track_id": "t3",
            "genre": "rock",
            "timestamp": "2024-01-01T00:00:20",
        },
    ]
    schema = (
        StructType()
        .add("user_id", StringType())
        .add("track_id", StringType())
        .add("genre", StringType())
        .add("timestamp", StringType())
    )
    df = spark.createDataFrame([Row(**row) for row in data], schema=schema)
    df = df.withColumn("timestamp", df["timestamp"].cast("timestamp"))

    agg_df = df.groupBy(window(df["timestamp"], "1 minute"), df["genre"]).count()
    # Flatten the window struct column for CSV output
    agg_df = (
        agg_df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )
    out_path = tmp_path / "out"
    agg_df.write.format("csv").option("header", True).save(str(out_path))
    files = list(out_path.glob("*.csv"))
    assert files, "No output CSV files found"

    found = pd.read_csv(files[0])
    assert set(found["genre"]) == {"pop", "rock"}
    assert found["count"].sum() == 3
    spark.stop()
