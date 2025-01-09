import pandas as pd
from datetime import datetime
from kafka import KafkaProducer # pip install kafka-python
from kafka import KafkaConsumer
import json

# Primero limpiamos y corregimos los datos fuente
data = pd.read_csv('data/london_houses.csv')
df = data.copy()
df = df.rename(columns={
    'Address': 'direccion', 
    'Neighborhood': 'vecindario', 
    'Bedrooms': 'n_dormitorios',
    'Bathrooms': 'n_banos', 
    'Square Meters': 'tamano', 
    'Building Age': 'edad_vivienda',
    'Garden': 'hay_jardin', 
    'Garage': 'hay_garaje', 
    'Floors': 'n_plantas',
    'Property Type': 'tipo_vivienda', 
    'Heating Type': 'tipo_calefaccion', 
    'Balcony': 'tipo_hay_terraza',
    'Interior Style': 'tipo_decorado', 
    'View': 'tipo_vistas', 
    'Materials': 'tipo_materiales',
    'Building Status': 'estado_vivienda',
    'Price (£)': 'precio_pounds'
})
df['hay_jardin'] = df['hay_jardin'].map({
    'Yes': True,
    'No': False
    })
df['hay_garaje'] = df['hay_garaje'].map({
    'Yes': True,
    'No': False
    })
df['precio_pounds'] = df['precio_pounds'].astype('float')
df['fecha_creacion'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
df['fecha_modificacion'] = None
df['fecha_baja'] = None
df['ano_construccion'] = datetime.today().year - df['edad_vivienda']
df.drop('edad_vivienda', axis=1, inplace=True)
df['precio_metro_cuadrado'] = df['precio_pounds'] / df['tamano']
df['precio_metro_cuadrado'] = df['precio_metro_cuadrado'].round(1)

# Una vez limpiados, en modo local con terminales trabajamos con Kafka
"""
abrimos dos terminales y
    vamos a la carpeta contenededora de kafka por ejemplo
    cd C:\kafka\bin\windows
en una terminal
activamos el servicio zookeeper
    .\zookeeper-server-start.bat ..\..\config\zookeeper.properties
en otra terminal
activamos un servidor de kafka
    .\kafka-server-start.bat ..\..\config\server.properties
y creamos un tópico donde un productor de Kafka enviará los mensajes (los datos limpiados del dataframe)
    kafka-topics.bat --create -topic activity2jurbina0 --bootstrap-server Judysu:9092 --partitions 1 --replication-factor 1

    si queremos hacer en modo local la creación de un tópico tiene que ser con KafkaAdminClient
from kafka.admin import KafkaAdminClient, NewTopic admin_client = KafkaAdminClient( bootstrap_servers="your_kafka_broker:9092", client_id='test_client' ) topic_list = [] topic_list.append(NewTopic(name="your_topic_name", num_partitions=1, replication_factor=1)) admin_client.create_topics(new_topics=topic_list, validate_only=False
    """
# continuamos con Python
# creamos y configuramos el productor de kafka
your_kafka_broker = 'Judysu:9092'
your_topic_name = 'activitidadCLOUD'
producer = KafkaProducer(
    bootstrap_servers=your_kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# enviamos mensajes al tópico de kafka
producer.send(your_topic_name, {
    'message': 'Hello, Kafka!'
    })

# creamos y configuramos el consumidor de kafka
consumer = KafkaConsumer(
    your_topic_name,
    bootstrap_servers=your_kafka_broker,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# comprobamos que el consumidor recibe los mensajes
for message in consumer:
    print(message.value)

# si miramos los datos a tiempo real
producer = KafkaProducer(
    bootstrap_servers=your_kafka_broker,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def process_message(message):
    """Ejemplo de un procesado simple de un mensaje"""
    print(message)
    return message

# volvemos a procesar los mensajes pero del nuevo consumidor
for message in consumer:
    mensaje_procesado = process_message(message.value)
    producer.send(your_topic_name, mensaje_procesado)
    producer.flush() # enviar los mensajes del buffer del productor al tópico


"""

# Sending data to Kafka topic
for _, row in df.iterrows():
    producer.send('your_topic_name', row.to_dict())

producer.flush()
"""

# queda modificar para que se procese en BATCH
# particiones con los temas y las particiones es lo que se distribuye entre brokers
# para mirar mi uclúster local, el bootstrap server y el Kafka broker en el archivo server.properties
# listeners=PLAINTEXT://localhost:9092
