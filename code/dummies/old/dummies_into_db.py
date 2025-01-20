from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import pymysql

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

# Configuración de Pyspark para conectarse a MySQL
# Ruta al archivo JAR del controlador JDBC
jdbc_jar_path = "code\pyspark_kafka\mysql-connector-j-9.1.0\mysql-connector-j-9.1.0.jar"

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

# Proceso ETL con PySpark hasta devolver lista de diccionarios
def etl_local_inmobiliaria(input_csv_path, mysql_table="viviendas"):
    """
    Realiza un proceso ETL sobre un archivo CSV y transforma los datos según las reglas especificadas.
    Devuelve una lista de diccionarios para insertar en MySQL.

    Args:
        input_csv_path (str): Ruta al archivo CSV de entrada.

    Returns:
        list: Lista de diccionarios con los datos transformados.
    """
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("ETL local Inmobiliaria") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()

    # Leer archivo CSV
    df_pyspark = spark.read.csv(input_csv_path, header=True, inferSchema=True)

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
    df = df.withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1))

    """
    # Agregar una columna con universal unique identifiers (UUIDs)
    df = df.withColumn("code_vivienda", F.expr("uuid()"))
    """
    # Leer tabla MySQL AWS
    df_mysql_aws = spark.read.format("jdbc") \
        .option("url", mysql_host) \
        .option("dbtable", mysql_table) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .load()
    # Leer tabla MySQL AWS
    df_mysql = spark.read.format("jdbc") \
        .option("url", db_host) \
        .option("dbtable", mysql_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()
    # Seleccionar solo la columna code_vivienda
    df_mysql = df_mysql.select("code_vivienda")
    # Realizar la comparación: encontrar registros que no están en la base de datos
    unmatched = df.join(
        df_mysql, 
        df["code_vivienda"] == df_mysql["code_vivienda"], 
        "left_anti"
        )
    # Convertir DataFrame a lista de diccionarios
    unmatched = [row.asDict() for row in df.collect()]

    # Detener la sesión de Spark
    spark.stop()

    return unmatched

data_dummies = etl_local_inmobiliaria('data/london_houses2_uuid.csv')
print("Se ha limpiado y comparado la muestra.")
print(data_dummies[:2])
