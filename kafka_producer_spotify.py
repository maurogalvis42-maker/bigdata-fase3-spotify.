import time
import json
import random
from kafka import KafkaProducer

artists = ["Bad Bunny", "Taylor Swift", "The Weeknd", "Karol G", "Drake", "Feid", "Shakira", "Peso Pluma"]
genres  = ["pop", "reggaeton", "hip-hop", "latin", "rock", "trap"]
songs   = ["Moscow Mule", "Cruel Summer", "Blinding Lights", "Provenza", "God's Plan", "Lejania", "Bzrp Session", "Ella Baila Sola"]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Productor de Spotify iniciado... enviando datos en tiempo real")
print("Presiona Ctrl+C para detener")

try:
    while True:
        data = {
            "track_name": random.choice(songs),
            "artist": random.choice(artists),
            "genre": random.choice(genres),
            "popularity": random.randint(40, 100),
            "duration_ms": random.randint(120000, 300000),
            "timestamp": int(time.time())
        }
        producer.send('spotify_plays', value=data)
        print(f"Sent: {data}")
        time.sleep(0.8)
except KeyboardInterrupt:
    print("\nProductor detenido.")
    producer.close()
