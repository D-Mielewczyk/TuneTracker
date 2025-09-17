from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


def main():
    spark = SparkSession.builder.appName("TuneTrackerAnalysis").getOrCreate()

    df = spark.read.csv("demo_output/results.csv", header=True, inferSchema=True)

    genre_counts = df.groupBy("genre").agg(_sum(col("count")).alias("total_plays"))

    top_genres = genre_counts.orderBy(col("total_plays").desc())

    top_genres.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
