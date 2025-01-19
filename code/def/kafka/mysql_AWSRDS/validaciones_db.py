import pymysql
db_host = "prototype-inmobiliaria.crs7uugzydvx.us-east-1.rds.amazonaws.com"
db_user = "root"
db_password = "rootpassword"
db_name = "sys"


def get_db_connection():
    """Connect to MySQL database."""
    try:
        print("Connecting to DataBase...")
        return pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
    except Exception as e:
        print(f"Error connecting to DataBase: {e}")
        return None
con = get_db_connection()
try:
    with con.cursor() as cursor:
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        cursor.execute("SHOW TABLES")
        table = cursor.fetchall()
        print("La versión es {}".format(data))
        print(table)
        print(table[0])
        print(table[0][0])
finally:
    con.commit()
    con.close()