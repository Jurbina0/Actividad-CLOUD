# Transformaciones en el DataFrame
import uuid
from datetime import datetime

# MySQL
import pymysql

# Spark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Configuración de conexión a MySQL local
db_host = "localhost"
db_user = "root"
db_password = "PROPILENO24a"
db_name = "inmobiliaria"

# Configuración de conexión a MySQL de AWS
mysql_host = 'prototype-inmobiliaria.crs7uugzydvx.us-east-1.rds.amazonaws.com'
mysql_user = 'root'
mysql_password = 'rootpassword'
mysql_name = 'inmobiliaria'


def get_db_connection_local():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_name
    )

# Creamos sesión de Spark
spark = SparkSession.builder \
    .appName("ETL local Inmobiliaria") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

df_pyspark = spark.read.csv("data\london_houses2_uuid.csv", header=True, inferSchema=True)

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

"""# Agregar una columna con universal unique identifiers (UUIDs)
transformed_df = transformed_df.withColumn(
    "code_vivienda", F.expr("uuid()")
)"""

# Convertir DataFrame a lista de diccionarios para inserción en MySQL
data = [row.asDict() for row in transformed_df.collect()]
# Función para insertar fila en la base de datos
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