import pymysql
import json
import datetime

# Database connection configuration
db_host = "localhost"
db_user = "root"
db_password = "PROPILENO24a"
db_name = "inmobiliaria"

def get_db_connection():
    """Create a connection to the MySQL database."""
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        connect_timeout=5
    )
    return connection

def insert_viviendas(df):
    try:
        # Open a database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        query = "algo"
        cursor.execute(query)
        connection.close()
    except Exception as e:
        print("Error connecting to the database: ", e)
        return None, None
connection = insert_viviendas()
print("Connection: ", connection)
