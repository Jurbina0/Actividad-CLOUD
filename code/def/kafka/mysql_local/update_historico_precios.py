import pymysql
db_host = "localhost"
db_user = "root"
db_password = "PROPILENO24a"
db_name = "inmobiliaria"

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
        query="""
        SELECT * FROM viviendas
        """
        cursor.execute(query)
        rows = cursor.fetchone()
        print(rows)
        cursor.execute("DESCRIBE viviendas")
        descript = cursor.fetchall()
        for col in descript:
            print(col)
        print("Historico Precios")
        cursor.execute("DESCRIBE historico_precios")
        for col in cursor.fetchall():
            print(col)
        cursor.execute("SELECT id_vivienda, precio_pounds, fecha_creacion, fecha_modificacion FROM viviendas")
        data_in_viviendas = cursor.fetchall()
        for row in data_in_viviendas:
            id_vivienda = row[0]
            precio_pounds = row[1]
            fecha_creacion = row[2]
            fecha_modificacion = row[3]
            row = data_in_viviendas[0]
            cursor.execute("INSERT INTO historico_precios (id_vivienda, precio_pounds, fecha_creacion, fecha_modificacion) VALUES (%s, %s, %s, %s)",
                       (id_vivienda, precio_pounds, fecha_creacion, fecha_modificacion))
        con.commit()
except Exception as e:
    print(f"Error de lectura: {e}")
con.close()