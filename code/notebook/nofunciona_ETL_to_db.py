import pymysql
import pandas as pd

# Configuración de conexión a la base de datos
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
        database=db_name,
        connect_timeout=5
    )

def values_to_list(data):
    """Convertir un DataFrame a una lista de tuplas para su inserción en la base de datos."""
    n_rows = data.shape[0]
    tuplas = []
    for row in range(n_rows):
        fila = data.iloc[row]
        valores = [
            None if pd.isnull(value) else int(value) if isinstance(value, bool) else value
            for value in fila.values
        ]
        registro = tuple(valores)
        tuplas.append(registro)
    return tuplas

def insert_data_from_csv(csv_file_path):
    """
    Insertar datos en la tabla 'viviendas' desde un archivo CSV.
    Args:
        csv_file_path (str): Ruta del archivo CSV.
    """
    try:
        # Abrir conexión a la base de datos
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Consulta de inserción
        insert_query = """
        INSERT INTO viviendas
        (id_vivienda, 
        direccion,
        vecindario, 
        n_dormitorios,
        n_banos,
        tamano,
        ano_construccion,
        hay_jardin,
        hay_garaje,
        n_plantas,
        tipo_vivienda,
        tipo_calefaccion,
        tipo_hay_terraza,
        tipo_decorado,
        tipo_vistas,
        tipo_materiales,
        estado_vivienda,
        precio_pounds,
        precio_metro_cuadrado,
        fecha_creacion,
        fecha_modificacion,
        fecha_baja)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Leer el archivo CSV
        data = pd.read_csv(csv_file_path)
        values = values_to_list(data)
        
        # Ejecutar la consulta
        cursor.executemany(insert_query, values)
        connection.commit()  # Confirmar cambios
        print(f"Se han insertado {cursor.rowcount} registros correctamente.")
        
    except Exception as e:
        print("Error al insertar los datos:", e)
    finally:
        # Cerrar conexión y cursor
        cursor.close()
        connection.close()

insertion = insert_data_from_csv('data/london_houses_clean.csv')