import pymysql
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
connection = get_db_connection()
try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        cursor.execute("SHOW TABLES")
        table = cursor.fetchall()
        cursor.execute("SHOW COLUMNS FROM {}".format(table[0][0]))
        columns = cursor.fetchall()
        cursor.execute("SELECT * FROM viviendas")
        rows = cursor.fetchmany(2)
        cursor.execute("DELETE FROM viviendas")
        cursor.execute("SELECT * FROM viviendas")
        afterrow = cursor.fetchall()
        colnames = [column[0] for column in columns]
        print("La versión es {}".format(data))
        print(table)
        print(table[0])
        print(table[0][0])
        print(colnames)
        for columns in columns:
            print(columns)
        print(colnames)
        print(rows)
        print(rows[0])
        print(rows[0][0])
        print(afterrow)
        
finally:
    connection.commit()
    connection.close()