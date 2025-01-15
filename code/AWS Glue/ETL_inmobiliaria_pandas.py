import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from datetime import datetime

# Inicializa el GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = glueContext.create_dynamic_frame.from_catalog

# Ruta del bucket origen y destino
input_path = "s3://m6-inmobiliaria-jm/raw_data/london_houses.csv"  # Cambiar según el bucket origen
output_path = "s3://m6-inmobiliaria-jm/processed_data/"  # Cambiar según el bucket destino

# Lee los datos del bucket S3 (en formato CSV)
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True}
)

# Convierte DynamicFrame a DataFrame para aplicar transformaciones personalizadas
df = datasource.toDF()

# Script ETL
# Hacemos operaciones como un dataframe de spark. Estas operaciones se hacen en los workers
# Renombrar columnas
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

# Pyspark al no tener categóricos , pasamos los categóricos a string
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

# Convierte de vuelta a DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")

# Escribe los datos transformados al bucket S3 destino
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv",
    format_options={"writeHeader": True}
)

print("ETL Job finalizado correctamente!")