import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from datetime import datetime
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)

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



# Ruta del bucket origen
input_path = f"s3://{s3_bucket}/{s3_raw_file}"  


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

df = df.withColumn("ano_construccion", F.lit(current_year) - F.col("edad_vivienda").cast("int")) \
       .drop("edad_vivienda") \
       .withColumn("precio_metro_cuadrado", F.round(F.col("precio_pounds") / F.col("tamano"), 1)) \


code_viviendas_list = df.select("code_vivienda").rdd.flatMap(lambda x: x).collect()
code_viviendas_list_string = str(code_viviendas_list).replace('[', '').replace(']', '').replace('"',"'")
logging.info(f"Lista viviendas repetidas: {code_viviendas_list_string}")

# Process Updates in MySQL
try:
    # Database Configuration
    mysql_host = db_host
    mysql_user = db_user
    mysql_password = db_password
    mysql_database = db_name
    mysql_table = "viviendas"
    
    # Open MySQL connection
    connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    with connection.cursor() as cursor:
        # Safe SQL Query to Fetch Existing Data
        query = """
            SELECT precio_metro_cuadrado, code_vivienda, id_vivienda, tamano
            FROM viviendas
            WHERE fecha_baja IS NULL
            AND code_vivienda IN %s;
        """
        
        # Pass the code_viviendas_list safely as a parameter
        code_viviendas_list = df.select("code_vivienda").rdd.flatMap(lambda x: x).collect()
        cursor.execute(query, (tuple(code_viviendas_list),))
        precios_viviendas = cursor.fetchall()
        
        # Extract existing `code_vivienda` from MySQL
        code_vivienda_list_from_db = [row["code_vivienda"] for row in precios_viviendas]
    
        # Filter DataFrame for repeated and new viviendas
        df_viviendas_repetidas = df.filter(F.col("code_vivienda").isin(code_vivienda_list_from_db))
        df_viviendas_nuevas = df.filter(~F.col("code_vivienda").isin(code_vivienda_list_from_db))
    
        # Log the counts
        logging.info(f"Número de viviendas repetidas: {df_viviendas_repetidas.count()}")
        logging.info(f"Número de viviendas nuevas: {df_viviendas_nuevas.count()}")
    
        # Handle repeated viviendas: Update if necessary
        if df_viviendas_repetidas.count() > 0:
            for row_nuevo in df_viviendas_repetidas.collect():
                for row_antes in precios_viviendas:
                    if row_antes["code_vivienda"] == row_nuevo["code_vivienda"]:
                        if row_antes["precio_metro_cuadrado"] != row_nuevo["precio_metro_cuadrado"]:
                            # Insert into historico_precios
                            cursor.execute("""
                                INSERT INTO historico_precios (
                                    precio_pounds,
                                    code_vivienda,
                                    id_vivienda
                                ) VALUES (%s, %s, %s)
                            """, (
                                row_nuevo["precio_pounds"],
                                row_nuevo["code_vivienda"],
                                row_antes["id_vivienda"]
                            ))
    
                            # Update viviendas
                            cursor.execute("""
                                UPDATE viviendas
                                SET precio_metro_cuadrado = %s, precio_pounds = %s
                                WHERE code_vivienda = %s
                            """, (
                                row_nuevo["precio_metro_cuadrado"],
                                row_nuevo["precio_pounds"],
                                row_nuevo["code_vivienda"]
                            ))
                            logging.info(f"Updated viviendas with code_vivienda: {row_nuevo['code_vivienda']}")
    
        # Handle new viviendas: Insert into MySQL
        if df_viviendas_nuevas.count() > 0:
            data = [row.asDict() for row in df_viviendas_nuevas.collect()]
            columns = ", ".join(data[0].keys())
            placeholders = ", ".join(["%s"] * len(data[0]))
            sql_query = f"INSERT INTO {mysql_table} ({columns}) VALUES ({placeholders})"
            cursor.executemany(sql_query, [tuple(row.values()) for row in data])
            
            time.sleep(5)
            
            # viviendas ingestadas
            code_viviendas_nuevas_list = df_viviendas_nuevas.select("code_vivienda").rdd.flatMap(lambda x: x).collect()
            query = """INSERT INTO historico_precios (id_vivienda, precio_pounds, code_vivienda)
                            SELECT id_vivienda, precio_pounds, code_vivienda
                            FROM viviendas
                            WHERE code_vivienda IN %s;
                            """
            cursor.execute(query, (tuple(code_viviendas_nuevas_list),))
            
            logging.info("Database inserts completed successfully.")
    
        # Commit changes
        connection.commit()
        
except Exception as e:
    logging.error(f"Error processing viviendas data: {e}")
finally:
    if connection:
        connection.close()

logging.info("ETL Job finalized successfully!")

