from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

# Necesarios para kafka
from kafka import KafkaProducer
import json
from kafka import KafkaConsumer

# a modo local es necesario abrir dos terminales y estar en la carpeta bin de kafka y ejecutar los siguientes comandos
"""
cd C:\kafka\bin\windows
.\zookeeper-server-start.bat ..\..\config\zookeeper.properties
cd C:\kafka\bin\windows
.\kafka-server-start.bat ..\..\config\server.properties
"""


# Necesarios para MySQL
import pymysql
from datetime import datetime


# Configuración de conexión a MySQL local
db_host = "localhost"
db_user = "root"
db_password = "PROPILENO24a"
db_name = "inmobiliaria"
def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

# Creamos sesión de Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Update historico_viviendas") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Detenemos la sesión de Spark
spark.stop()

# Configuración de Kafka
kafka_bootstrap_server = 'Judysu:9092'
kafka_topic = 'activitidadCLOUD'
group_id = 'group1'

# Función que avisa a Kafka y hace que el productor envíe un mensaje
def notify_kafka_with_price_per_sqm():
    try:
        # Conexión a la base de datos
        connection = get_db_connection()
        cursor = connection.cursor()
        # Consulta para obtener el último registro insertado en historico_precios
        cursor.execute("""
            SELECT hp.id_vivienda, hp.precio_pounds, v.tamano 
            FROM historico_precios hp
            INNER JOIN viviendas v
                       ON hp.id_vivienda = v.id_vivienda
            ORDER BY hp.id_precio DESC LIMIT 1
        """)
        last_insert = cursor.fetchone()

        if last_insert:
            id_vivienda = last_insert[0]
            precio_pounds = last_insert[1]
            tamano = last_insert[2]

            # Calcular precio por metro cuadrado
            precio_metro_cuadrado = round(precio_pounds / tamano, 1) if tamano > 0 else 0

            # Crear mensaje para Kafka
            message = {
                "id_vivienda": id_vivienda,
                "precio_pounds": precio_pounds,
                "precio_metro_cuadrado": precio_metro_cuadrado,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            # Productor Kafka
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8') 
            )
            producer.send(kafka_topic, value=message)
            producer.flush()
            producer.close()
            print(f"Mensaje enviado a Kafka: {message}")
        else:
            print("No se encontró ningún registro reciente para enviar a Kafka.")
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka: {e}")

def process_kafka_messages():
    """Procesa los mensajes del consumidor de Kafka y actualiza precio_metro_cuadrado en la base de datos."""
    # Consumidor Kafka
    consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_server,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        # Iterar sobre los mensajes del consumidor
        for message in consumer:
            data = message.value
            id_vivienda = data["id_vivienda"]
            precio_metro_cuadrado = data["precio_metro_cuadrado"]
            cursor.execute("""
                    UPDATE viviendas
                    SET precio_metro_cuadrado = %s
                    WHERE id_vivienda = %s
                    """, (precio_metro_cuadrado, id_vivienda))
            print(f"Actualizado precio_metro_cuadrado a {precio_metro_cuadrado} para id_vivienda {id_vivienda}")
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error procesando mensajes de Kafka: {e}")
    # Finalización y cierre del consumidor
    print("Fin del proceso de actualización.")
    consumer.close()


def fetch_precio_pounds():
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT id_vivienda, precio_pounds, fecha_creacion FROM viviendas")
        current_price = cursor.fetchall()
        cursor.execute("SELECT id_vivienda, precio_pounds, fecha_creacion FROM historico_precios")
        historical_price = cursor.fetchall()
        for id, prices in enumerate(current_price):
            if prices[1]!=historical_price[id][1]:
                cursor.execute("INSERT INTO historico_precios (id_vivienda, precio_pounds, fecha_creacion) VALUES (%s, %s, %s)", (prices[0], prices[1], prices[2]))
                notify_kafka_with_price_per_sqm()
                process_kafka_messages()
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error al insertar fila: {e}")

fetch_precio_pounds()

