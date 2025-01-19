from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
# Necesarios para kafka
from kafka import KafkaProducer
import json
# a modo local es necesario abrir dos terminales
"""
.\zookeeper-server-start.bat ..\..\config\zookeeper.properties
.\kafka-server-start.bat ..\..\config\server.properties
"""
# Creamos sesión de Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL local Inmobiliaria") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

df_pyspark = spark.read.csv("data/london_houses.csv", header=True, inferSchema=True)
df_pyspark.printSchema()

# ETL

# Renombrar columnas
df = df_pyspark.withColumnRenamed("Address", "direccion") \
       .withColumnRenamed("Neighborhood", "vecindario") \
       .withColumnRenamed("Bedrooms", "n_dormitorios") \
       .withColumnRenamed("Bathrooms", "n_banos") \
       .withColumnRenamed("Square Meters", "tamano") \
       .withColumnRenamed("Building Age", "edad_vivienda") \
       .withColumnRenamed("Garden", "hay_jardin") \
       .withColumnRenamed("Garage", "hay_garaje") \
       .withColumnRenamed("Floors", "n_plantas") \
       .withColumnRenamed("Property Type", "tipo_vivienda") \
       .withColumnRenamed("Heating Type", "tipo_calefaccion") \
       .withColumnRenamed("Balcony", "tipo_hay_terraza") \
       .withColumnRenamed("Interior Style", "tipo_decorado") \
       .withColumnRenamed("View", "tipo_vistas") \
       .withColumnRenamed("Materials", "tipo_materiales") \
       .withColumnRenamed("Building Status", "estado_vivienda") \
       .withColumnRenamed("Price (£)", "precio_pounds")

# Convertir columnas 'Yes/No' a booleanas
df = df.withColumn("hay_jardin", F.when(F.col("hay_jardin") == "Yes", True).otherwise(False)) \
       .withColumn("hay_garaje", F.when(F.col("hay_garaje") == "Yes", True).otherwise(False))

# Convertir categóricos a string
categorical_columns = ['tipo_hay_terraza', 'tipo_vivienda', 'tipo_calefaccion', 
                       'tipo_decorado', 'tipo_vistas', 'tipo_materiales', 'estado_vivienda']
for colname in categorical_columns:
    df = df.withColumn(colname, F.col(colname).cast("string"))

# Convertir 'precio_pounds' a float
df = df.withColumn("precio_pounds", F.col("precio_pounds").cast("float"))

# Agregar fecha de creación
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df = df.withColumn("fecha_creacion", F.lit(current_datetime))

# Agregar columnas vacías
df = df.withColumn("fecha_modificacion", F.lit(None).cast("string")) \
       .withColumn("fecha_baja", F.lit(None).cast("string"))

# Calcular el año de construcción
current_year = datetime.now().year
df = df.withColumn("ano_construccion", F.lit(current_year) - F.col("edad_vivienda").cast("int"))

# Eliminar la columna 'edad_vivienda'
df = df.drop("edad_vivienda")

# Calcular el precio por metro cuadrado y redondear
transformed_df = df.withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1))
transformed_df.show(1)

# convertimos el dataframe a diccionario
rows = transformed_df.collect()
dict = rows[0].asDict()
print(rows[0].asDict())


# Configuración de Kafka
kafka_bootstrap_server = 'Judysu:9092'
kafka_topic = 'activitidadCLOUD'
group_id = 'group1'

# Consumidor Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_server,
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
)

# Enviar datos del DataFrame al tópico de Kafka
def send_to_kafka(sparkDataFrame):
    rows = sparkDataFrame.collect()  # Convertir a lista de filas
    for row in rows:
        # Convertir cada fila a un diccionario
        row_dict = row.asDict()
        # Enviar el mensaje a Kafka
        producer.send(kafka_topic, value=row_dict)
        print(f"Mensaje enviado: {row_dict}")

# Enviar los datos transformados a Kafka
send_to_kafka(transformed_df)


# Detenemos la sesión de Spark
spark.stop()
# Detenemos el productor de Kafka
producer.close()
