import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import datetime
import pyspark.sql.functions as F

# import pyspark.sql.types as T
# Definimos UDF para generar UUIDs generate_uuid = F.udf(lambda: str(uuid.uuid4()), T.StringType())

# Librería adicional para conectar con MySQL
import pymysql

## @params: [JOB_NAME]
# Inicializa el GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket','s3_raw_file','db_host','db_user', 'db_password', 'db_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_bucket = args['s3_bucket']
s3_raw_file = args['s3_raw_file']
db_host = args['db_host']
db_user = args['db_user']
db_password = args['db_password']
db_name = args['db_name']



# Ruta del bucket origen y destino
input_path = f"s3://{s3_bucket}/raw_data/{s3_raw_file}"  # Cambiar según el bucket origen
output_path = f"s3://{s3_bucket}/processed_data/"  # Cambiar según el bucket destino

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
       .withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1)) \
       .withColumn("code_vivienda", F.expr("uuid()")) # Agregar una columna con universal unique identifiers (UUIDs)

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

def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

# Convertir DataFrame a lista de diccionarios para inserción en MySQL
data = [row.asDict() for row in df.collect()]
# Function to insert rows into the database
def insert_rows(data):
    try:
        connection = get_db_connection()  # Assuming this function is defined elsewhere
        cursor = connection.cursor()

        # Prepare the SQL query for viviendas
        viviendas_insert_query = """
            INSERT INTO viviendas (
                direccion, vecindario, n_dormitorios, n_banos, tamano, hay_jardin,
                hay_garaje, n_plantas, tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
                tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda, precio_pounds,
                fecha_creacion, ano_construccion, precio_metro_cuadrado, code_vivienda
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepare the SQL query for historico_precios
        historico_insert_query = """
            INSERT INTO historico_precios (
                precio_pounds,
                fecha_creacion, 
                id_vivienda,
                code_vivienda
            ) VALUES (%s, %s, %s, %s)
        """

        for row in data:
            # Prepare values for viviendas
            viviendas_values = (
                row.get('direccion', None),
                row.get('vecindario', None),
                row.get('n_dormitorios', None),
                row.get('n_banos', None),
                row.get('tamano'),
                row.get('hay_jardin'),
                row.get('hay_garaje'),
                row.get('n_plantas'),
                row.get('tipo_vivienda', None),
                row.get('tipo_calefaccion', None),
                row.get('tipo_hay_terraza', None),
                row.get('tipo_decorado', None),
                row.get('tipo_vistas', None),
                row.get('tipo_materiales', None),
                row.get('estado_vivienda', None),
                row.get('precio_pounds'),
                row.get('fecha_creacion'),
                row.get('ano_construccion'),
                row.get('precio_metro_cuadrado'),
                row.get('code_vivienda')
            )
            
            # Insert into viviendas table
            cursor.execute(viviendas_insert_query, viviendas_values)
            connection.commit()  # Commit after each row for transactional integrity

            # Get the ID of the inserted vivienda
            cursor.execute("SELECT LAST_INSERT_ID() AS id_vivienda;")
            result = cursor.fetchone()
            id_vivienda = result[0]

            # Prepare values for historico_precios
            historico_values = (
                row.get('precio_pounds'),
                row.get('fecha_creacion'),
                id_vivienda,  # Use the generated id_vivienda
                row.get('code_vivienda')
            )

            # Insert into historico_precios table
            cursor.execute(historico_insert_query, historico_values)
            print(f"Row for vivienda ID {id_vivienda} inserted into historico_precios.")

        connection.commit()  # Final commit after all rows are processed
        print(f"All rows inserted successfully.")
    except Exception as e:
        print(f"Error inserting rows: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
insert_rows(data)