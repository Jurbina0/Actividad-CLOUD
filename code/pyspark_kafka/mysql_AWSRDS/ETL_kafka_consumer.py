from kafka import KafkaConsumer
import pymysql
import json
from datetime import datetime


# Configuración de Kafka
kafka_bootstrap_server = 'Judysu:9092'
kafka_topic = 'activitidadCLOUD'
group_id = 'group1'

# Configuración de conexión a MySQL
db_host = "prototype-inmobiliaria.crs7uugzydvx.us-east-1.rds.amazonaws.com"
db_user = "root"
db_password = "rootpassword"
db_name = "inmobiliaria"


def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

# Función para insertar fila en la base de datos
def insert_row(row):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        # Insertamos en la tabla viviendas
        insert_query = """
            INSERT INTO viviendas (
                direccion, vecindario, n_dormitorios, n_banos, tamano, hay_jardin,
                hay_garaje, n_plantas, tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
                tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda, precio_pounds,
                fecha_creacion, ano_construccion, precio_metro_cuadrado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            row['direccion'], 
            row['vecindario'], 
            int(row['n_dormitorios']), 
            int(row['n_banos']),
            float(row['tamano']),
            row['hay_jardin'] == 'TRUE', # se inserta como booleano
            row['hay_garaje'] == 'TRUE', # se inserta como booleano
            int(row['n_plantas']), 
            row['tipo_vivienda'], 
            row['tipo_calefaccion'], 
            row['tipo_hay_terraza'],
            row['tipo_decorado'], 
            row['tipo_vistas'],
            row['tipo_materiales'], 
            row['estado_vivienda'],
            float(row['precio_pounds']),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            int(row['ano_construccion']),
            float(row['precio_metro_cuadrado'])
        )
        cursor.execute(insert_query, values)
        connection.commit()
    except Exception as e:
        print(f"Error al insertar fila: {e}")
    finally:
        cursor.close()
        connection.close()

# Consumidor Kafka
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_server,
    group_id=group_id,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserializa mensajes JSON
)

# Espera mensajes del tópico de Kafka
for message in consumer:
    row = message.value
    insert_row(row)
    
# Finalización
print("Fin de insertar.")
# Cerramos el consumidor
consumer.close()