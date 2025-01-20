from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round
from datetime import datetime
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Transformaciones en PySpark").getOrCreate()

# Leer el archivo CSV
file_path = "data/london_houses.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

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
df = df.withColumn("hay_jardin", when(col("hay_jardin") == "Yes", True).otherwise(False)) \
       .withColumn("hay_garaje", when(col("hay_garaje") == "Yes", True).otherwise(False))

# Pyspark al no tener categóricos , pasamos los categórigos a string
categorical_columns = ['tipo_hay_terraza', 'tipo_vivienda', 'tipo_calefaccion', 
                       'tipo_decorado', 'tipo_vistas', 'tipo_materiales', 'estado_vivienda']
for colname in categorical_columns:
    df = df.withColumn(colname, col(colname).cast("string"))

# Convertir 'precio_pounds' a float
df = df.withColumn("precio_pounds", col("precio_pounds").cast("float"))

# Agregar fecha de creación
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df = df.withColumn("fecha_creacion", lit(current_datetime))

# Agregar columnas vacías
df = df.withColumn("fecha_modificacion", lit(None).cast("string")) \
       .withColumn("fecha_baja", lit(None).cast("string"))

# Calcular el año de construcción
current_year = datetime.now().year
df = df.withColumn("ano_construccion", lit(current_year) - col("edad_vivienda").cast("int"))

# Eliminar la columna 'edad_vivienda'
df = df.drop("edad_vivienda")

# Calcular el precio por metro cuadrado y redondear
df = df.withColumn("precio_metro_cuadrado", round(col("precio_pounds") / col("tamano"), 1))

# Guardar el DataFrame en un archivo CSV limpio
output_path = "data/london_houses_pyspark.csv"
df.write.csv(output_path, header=True, mode="overwrite")

