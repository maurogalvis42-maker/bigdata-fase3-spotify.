from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SpotifyStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("track_name", StringType()),
    StructField("artist", StringType()),
    StructField("genre", StringType()),
    StructField("popularity", IntegerType()),
    StructField("duration_ms", IntegerType()),
    StructField("timestamp", LongType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spotify_plays") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
           .withColumn("event_time", col("timestamp").cast("timestamp"))

# Análisis en tiempo real: conteo de reproducciones y popularidad promedio por género cada 30 segundos
stats = parsed \
    .groupBy(window(col("event_time"), "30 seconds"), "genre") \
    .agg(
        count("*").alias("plays"),
        avg("popularity").alias("avg_popularity")
    )

query = stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("Consumidor Spark Streaming iniciado. Esperando datos del topic 'spotify_plays'...")
query.awaitTermination()
