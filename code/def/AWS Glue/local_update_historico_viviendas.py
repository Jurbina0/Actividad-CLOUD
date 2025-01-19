from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import pymysql

# Configuración de conexión a MySQL de AWS
mysql_host = 'prototype-inmobiliaria.crs7uugzydvx.us-east-1.rds.amazonaws.com'
mysql_user = 'root'
mysql_password = 'rootpassword'
mysql_name = 'inmobiliaria'


def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_name
    )

# Proceso ETL con PySpark hasta devolver lista de diccionarios
def etl_local_inmobiliaria(input_csv_path):
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
        .getOrCreate()

    # Leer archivo CSV
    df_pyspark = spark.read.csv(input_csv_path, header=True, inferSchema=True)
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
    new_data = [row.asDict() for row in df.collect()]
    spark.stop()
    try:
        connection = get_db_connection()  # Assuming this function is defined elsewhere
        cursor = connection.cursor()
        # comparativa de precios
        cursor.execute("""
                       SELECT precio_metro_cuadrado, code_vivienda, id_vivienda, tamano
                       FROM viviendas
                       """) # en histórico precios no hay precio_metro_cuadrado
        precios_viviendas = cursor.fetchall()
        for row_antes in precios_viviendas:
            for row_nuevo in new_data:
                if row_antes[1] == row_nuevo['code_vivienda']:
                    if row_antes[0] != row_nuevo['precio_metro_cuadrado']:
                        query = """
                                INSERT INTO historico_precios (
                                    precio_pounds,
                                    code_vivienda,
                                    id_vivienda
                                ) VALUES (%s, %s, %s)
                                """
                        values = (
                            row_nuevo['precio_metro_cuadrado']*row_antes[3],
                            row_nuevo['code_vivienda'],
                            row_antes[2]
                        )
                        cursor.execute(query, values)
                        print(f"Inserted row with code_vivienda: {row_nuevo['code_vivienda']}")
                         # actualizamos la tabla viviendas
                
                        query_update = """
                            UPDATE viviendas
                            SET precio_metro_cuadrado = %s, precio_pounds = %s
                            WHERE code_vivienda = %s
                            """
                        values_update = (
                            row_nuevo['precio_metro_cuadrado'],
                            row_nuevo['precio_pounds'],
                            row_nuevo['code_vivienda']
                        )
                        cursor.execute(query_update, values_update)
                        print(f"Updated viviendas with code_vivienda: {row_nuevo['code_vivienda']}")
        connection.commit()
        print(f"All rows inserted successfully.")
    except Exception as e:
        print(f"Error inserting rows: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
input_csv_path = "data\london_houses2_uuid.csv"
etl_local_inmobiliaria(input_csv_path)