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
def get_db_connection():
    """Crear una conexión a la base de datos MySQL."""
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )

# Creamos sesión de Spark
spark = SparkSession.builder \
    .appName("ETL local Inmobiliaria") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

df_pyspark = spark.read.csv("data/london_houses.csv", header=True, inferSchema=True)

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

# Agregar una columna con universal unique identifiers (UUIDs)
transformed_df = transformed_df.withColumn(
    "code_vivienda", F.expr("uuid()")
)

# Convertir DataFrame a lista de diccionarios para inserción en MySQL
data = [row.asDict() for row in transformed_df.collect()]
# Función para insertar fila en la base de datos
def insert_rows(data):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Prepare the SQL query
        insert_query = """
            INSERT INTO viviendas (
                direccion, vecindario, n_dormitorios, n_banos, tamano, hay_jardin,
                hay_garaje, n_plantas, tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
                tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda, precio_pounds,
                fecha_creacion, ano_construccion, precio_metro_cuadrado, code_vivienda
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in data:
            # Prepare the values for all rows
            values =(
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
            # Insertamos la fila
            cursor.execute(insert_query, values)
            print(f"Fila {row} insertada correctamente.")
        connection.commit()
        print(f"Rows inserted successfully.")
    except Exception as e:
        print(f"Error inserting rows: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
insert_rows(data)