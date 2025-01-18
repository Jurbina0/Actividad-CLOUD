from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import pymysql

db_host_jdbc = "jdbc:mysql://localhost:3306/inmobiliaria"
db_user = "root"
db_password = "PROPILENO24a"
db_name = "inmobiliaria"
db_table = "viviendas"

mysql_host = "jdbc:mysql://prototype-inmobiliaria.crs7uugzydvx.us-east-1.rds.amazonaws.com:3306/inmobiliaria"
mysql_table = "viviendas"
mysql_user = 'root'
mysql_password = 'rootpassword'
mysql_name = 'inmobiliaria'

# Ruta al archivo JAR del controlador JDBC
jdbc_jar_path = "code\pyspark_kafka\mysql-connector-j-9.1.0\mysql-connector-j-9.1.0.jar"

# Crear sesión de Spark
spark = SparkSession.builder \
        .appName("ETL local Inmobiliaria") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.jars", jdbc_jar_path) \
        .getOrCreate()
try:
    df_mysql_aws = spark.read.format("jdbc") \
        .option("url", db_host_jdbc) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()
    df_mysql_aws.show(5)
except Exception as e:
    print(f"Error al conectar a MySQL AWS: {e}")
print(spark.sparkContext.getConf().getAll())