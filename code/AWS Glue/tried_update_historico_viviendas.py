import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from datetime import datetime
import pyspark.sql.functions as F
import pymysql

# Glue and S3 Configuration
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket', 's3_raw_file', 'db_host', 'db_user', 'db_password', 'db_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Extract Parameters
s3_bucket = args['s3_bucket']
s3_raw_file = args['s3_raw_file']
db_host = args['db_host']
db_user = args['db_user']
db_password = args['db_password']
db_name = args['db_name']
input_path = f"s3://{s3_bucket}/{s3_raw_file}"  # S3 Input Path
output_path = f"s3://{s3_bucket}/processed_data/"  # S3 Output Path

def get_db_connection():
    """Create a connection to the MySQL database."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )

def etl_inmobiliaria(input_path):
    """
    Perform ETL for real estate data from S3 and process updates in MySQL.

    Args:
        input_path (str): S3 input file path.

    Returns:
        None
    """
    # Read Data from S3
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="csv",
        format_options={"withHeader": True}
    )
    df = datasource.toDF()

    # Data Transformations
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
           .withColumnRenamed("Price (£)", "precio_pounds") \
           .withColumn("hay_jardin", F.when(F.col("hay_jardin") == "Yes", True).otherwise(False)) \
           .withColumn("hay_garaje", F.when(F.col("hay_garaje") == "Yes", True).otherwise(False)) \
           .withColumn("precio_pounds", F.col("precio_pounds").cast("float")) \
           .withColumn("fecha_creacion", F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) \
           .withColumn("fecha_modificacion", F.lit(None).cast("string")) \
           .withColumn("fecha_baja", F.lit(None).cast("string")) \
           .withColumn("ano_construccion", F.lit(datetime.now().year) - F.col("edad_vivienda").cast("int")) \
           .drop("edad_vivienda") \
           .withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1))

    # Write Transformed Data to S3
    transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="csv",
        format_options={"writeHeader": True}
    )

    # Convert to List of Dictionaries for MySQL Processing
    new_data = [row.asDict() for row in df.collect()]

    # Process Updates in MySQL
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Fetch Existing Data from `viviendas`
        cursor.execute("""
            SELECT precio_metro_cuadrado, code_vivienda, id_vivienda, tamano
            FROM viviendas
            WHERE fecha_baja IS NULL;
        """)
        precios_viviendas = cursor.fetchall()

        for row_antes in precios_viviendas:
            for row_nuevo in new_data:
                if row_antes['code_vivienda'] == row_nuevo['code_vivienda']:
                    if row_antes['precio_metro_cuadrado'] != row_nuevo['precio_metro_cuadrado']:
                        # Insert into historico_precios
                        cursor.execute("""
                            INSERT INTO historico_precios (
                                precio_pounds,
                                code_vivienda,
                                id_vivienda
                            ) VALUES (%s, %s, %s)
                        """, (
                            row_nuevo['precio_pounds'],
                            row_nuevo['code_vivienda'],
                            row_antes['id_vivienda']
                        ))

                        # Update viviendas
                        cursor.execute("""
                            UPDATE viviendas
                            SET precio_metro_cuadrado = %s, precio_pounds = %s
                            WHERE code_vivienda = %s
                        """, (
                            row_nuevo['precio_metro_cuadrado'],
                            row_nuevo['precio_pounds'],
                            row_nuevo['code_vivienda']
                        ))
                        print(f"Updated viviendas with code_vivienda: {row_nuevo['code_vivienda']}")

        connection.commit()
        print("Database updates completed successfully.")

    except Exception as e:
        print(f"Error processing database updates: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

etl_inmobiliaria(input_path)