Procesamiento de Datos Spotify 

Descripción

Aplicación que realiza:
- Procesamiento en Batch: carga, limpieza y análisis exploratorio (EDA) del dataset de Spotify usando PySpark.
- Procesamiento en Tiempo Real: simulación de eventos de reproducción de canciones mediante Kafka + análisis con Spark Structured Streaming, conteo de plays y popularidad promedio por género.

Requisitos

- Ubuntu Server 22.04
- Java 11
- Apache Spark 3.5.8
- Apache Kafka 3.9.2
- Python 3 + librería `kafka-python`

Dataset

- Archivo: `spotify_songs.csv` (Spotify Songs - TidyTuesday)
- Contiene información de canciones: nombre, artista, popularidad, género de playlist, danceability, energy, etc.


Cómo ejecutar

1. Procesamiento en Batch

``bash
spark-submit batch_spotify.py
``

2. Procesamiento en Tiempo Real

Terminal 1 - Arrancar ZooKeeper y Kafka (si no están corriendo):
``bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
``

Crear el topic (solo la primera vez):
``bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic spotify_plays
`

Terminal 1 - Productor:
``bash
python3 kafka_producer_spotify.py
``

Terminal 2 - Consumidor Spark Streaming:
``bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_spotify.py
``

Spark UI
Una vez corriendo el job de Spark, acceder a:  
`http://192.168.1.7:4040`

Autor

Andres Mauricio Galvis Gutierrez  
Grupo 66 - UNAD
