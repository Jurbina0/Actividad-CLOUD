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
    """Crear una conexión a la base de datos MySQL local."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

def get_db_connection():
    """Crear una conexión a la base de datos MySQL en AWS."""
    return pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_name
    )

# Creamos una sesión de Spark
spark = SparkSession.builder \
    .appName("ETL local Inmobiliaria") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Leer el archivo CSV usando PySpark
df_pyspark = spark.read.csv("data/london_houses2_uuid.csv", header=True, inferSchema=True)

# *** Proceso ETL ***

# 1. Renombrar columnas
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

# 2. Convertir columnas 'Yes/No' a valores booleanos
df = df.withColumn("hay_jardin", F.when(F.col("hay_jardin") == "Yes", True).otherwise(False)) \
       .withColumn("hay_garaje", F.when(F.col("hay_garaje") == "Yes", True).otherwise(False))

# 3. Convertir columnas categóricas a tipo string
categorical_columns = ['tipo_hay_terraza', 'tipo_vivienda', 'tipo_calefaccion', 
                       'tipo_decorado', 'tipo_vistas', 'tipo_materiales', 'estado_vivienda']
for colname in categorical_columns:
    df = df.withColumn(colname, F.col(colname).cast("string"))

# 4. Convertir 'precio_pounds' a tipo float
df = df.withColumn("precio_pounds", F.col("precio_pounds").cast("float"))

# 5. Agregar la columna 'fecha_creacion' con la fecha actual
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df = df.withColumn("fecha_creacion", F.lit(current_datetime))

# 6. Agregar columnas vacías
df = df.withColumn("fecha_modificacion", F.lit(None).cast("string")) \
       .withColumn("fecha_baja", F.lit(None).cast("string"))

# 7. Calcular el año de construcción
current_year = datetime.now().year
df = df.withColumn("ano_construccion", F.lit(current_year) - F.col("edad_vivienda").cast("int"))

# 8. Eliminar la columna 'edad_vivienda'
df = df.drop("edad_vivienda")

# 9. Calcular el precio por metro cuadrado y redondear
transformed_df = df.withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1))

# Convertir el DataFrame transformado a una lista de diccionarios
data = [row.asDict() for row in transformed_df.collect()]

# *** Función para insertar filas en la base de datos MySQL ***
def insert_rows(data):
    try:
        connection = get_db_connection()  # Conectar a la base de datos
        cursor = connection.cursor()

        # Query para insertar en la tabla 'viviendas'
        viviendas_insert_query = """
            INSERT INTO viviendas (
                direccion, vecindario, n_dormitorios, n_banos, tamano, hay_jardin,
                hay_garaje, n_plantas, tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
                tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda, precio_pounds,
                fecha_creacion, ano_construccion, precio_metro_cuadrado, code_vivienda
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Query para insertar en la tabla 'historico_precios'
        historico_insert_query = """
            INSERT INTO historico_precios (
                precio_pounds,
                fecha_creacion, 
                id_vivienda,
                code_vivienda
            ) VALUES (%s, %s, %s, %s)
        """

        for row in data:
            # Valores para insertar en 'viviendas'
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

            # Insertar en 'viviendas'
            cursor.execute(viviendas_insert_query, viviendas_values)
            connection.commit()

            # Obtener el ID de la fila insertada
            cursor.execute("SELECT LAST_INSERT_ID() AS id_vivienda;")
            result = cursor.fetchone()
            id_vivienda = result['id_vivienda']

            # Valores para insertar en 'historico_precios'
            historico_values = (
                row.get('precio_pounds'),
                row.get('fecha_creacion'),
                id_vivienda,
                row.get('code_vivienda')
            )

            # Insertar en 'historico_precios'
            cursor.execute(historico_insert_query, historico_values)
            print(f"Fila con id_vivienda {id_vivienda} insertada en historico_precios.")
        connection.commit()
    except Exception as e:
        print(f"Error al insertar filas: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Llamada a la función para insertar los datos transformados
insert_rows(data)