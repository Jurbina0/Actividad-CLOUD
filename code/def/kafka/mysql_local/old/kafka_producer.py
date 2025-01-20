import csv
from kafka import KafkaProducer
import json

# Configuración de Kafka
kafka_bootstrap_server = 'Judysu:9092'
kafka_topic = 'activitidadCLOUD'

# Crear productor Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_server,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializa los mensajes a JSON
)

def produce_csv_messages(csv_file_path):
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)  # Leer filas como diccionarios
        for row in reader:
            producer.send(kafka_topic, value=row)
            print(f"Mensaje enviado: {row}")

# Ruta al archivo CSV
csv_file_path = "data\london_houses_clean.csv"
produce_csv_messages(csv_file_path)