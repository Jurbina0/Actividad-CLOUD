import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from datetime import datetime

# Librería adicional para conectar con MySQL
import pymysql

# Inicializa el GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket','s3_raw_file','db_host','db_user', 'db_password', 'db_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3_bucket = args['s3_bucket']
s3_raw_file = args['s3_raw_file']
db_host = args['db_host']
db_user = args['db_user']
db_password = args['db_password']
db_name = args['db_name']



# Ruta del bucket origen y destino
input_path = f"s3://{s3_bucket}/{s3_raw_file}"  # Cambiar según el bucket origen
output_path = "s3://m6-inmobiliaria-mj/processed_data/"  # Cambiar según el bucket destino

# Leer los datos del bucket S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Convertir DynamicFrame a DataFrame
df = datasource.toDF()

# Transformaciones en el DataFrame
df = df.withColumnRenamed("Address", "direccion") \
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

# Convertir columnas categóricas a string
categorical_columns = ['tipo_hay_terraza', 'tipo_vivienda', 'tipo_calefaccion', 
                       'tipo_decorado', 'tipo_vistas', 'tipo_materiales', 'estado_vivienda']
for colname in categorical_columns:
    df = df.withColumn(colname, F.col(colname).cast("string"))

# Convertir 'precio_pounds' a float
df = df.withColumn("precio_pounds", F.col("precio_pounds").cast("float"))

# Agregar columnas calculadas
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
current_year = datetime.now().year

df = df.withColumn("fecha_creacion", F.lit(current_datetime)) \
       .withColumn("fecha_modificacion", F.lit(None).cast("string")) \
       .withColumn("fecha_baja", F.lit(None).cast("string")) \
       .withColumn("ano_construccion", F.lit(current_year) - F.col("edad_vivienda").cast("int")) \
       .drop("edad_vivienda") \
       .withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1))

# Escribir los datos transformados en S3
transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv",
    format_options={"writeHeader": True}
)

# **Escribir en MySQL**
# Configuración de conexión a MySQL
mysql_host = db_host
mysql_user = db_user
mysql_password = db_password
mysql_database = db_name
mysql_table = "viviendas"

# Convertir DataFrame a lista de diccionarios para inserción en MySQL
data = [row.asDict() for row in df.collect()]

# Conectar a la base de datos MySQL y escribir los datos
try:
    connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        cursorclass=pymysql.cursors.DictCursor
    )
    with connection.cursor() as cursor:
        # Crear la consulta SQL para insertar los datos
        columns = ", ".join(data[0].keys())
        placeholders = ", ".join(["%s"] * len(data[0]))
        sql_query = f"INSERT INTO {mysql_table} ({columns}) VALUES ({placeholders})"
        
        # Ejecutar la inserción en lote
        cursor.executemany(sql_query, [tuple(row.values()) for row in data])
        connection.commit()
    print("Datos insertados correctamente en MySQL.")
except Exception as e:
    print(f"Error al insertar datos en MySQL: {e}")
finally:
    if connection:
        connection.close()

print("ETL Job finalizado correctamente!")
