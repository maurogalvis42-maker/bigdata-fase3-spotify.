from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

spark = SparkSession.builder.appName("SpotifyBatch").getOrCreate()

# Cargar el dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("spotify_songs.csv")

print("=== Schema ===")
df.printSchema()

print("\n=== Muestra de datos ===")
df.show(5)

print("\n=== Total de canciones ===")
print(df.count())

print("\n=== Top 10 canciones más populares ===")
df.orderBy(desc("track_popularity")).select("track_name", "track_artist", "track_popularity").show(10)

print("\n=== Promedio de popularidad por género ===")
df.groupBy("playlist_genre") \
  .agg(avg("track_popularity").alias("avg_popularity"), count("*").alias("num_tracks")) \
  .orderBy(desc("avg_popularity")).show()

print("\n=== Estadísticas de danceability, energy y valence ===")
df.select("danceability", "energy", "valence").describe().show()

spark.stop()
