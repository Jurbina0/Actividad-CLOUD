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

# insertar una fila en la base de datos
def insert_row(values):
    try:
        # Abrir conexión a la base de datos
        connection = get_db_connection()
        cursor = connection.cursor()
        # Consulta de inserción
        insert_query = """
            INSERT INTO viviendas (
    direccion,
    vecindario, 
    n_dormitorios,
    n_banos,
    tamano,
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
    fecha_creacion,
    ano_construccion,
    precio_metro_cuadrado
) 
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
        """
        # Ejecutar la consulta
        cursor.execute(insert_query, values)
        connection.commit()  # Confirmar cambios
        print(f"Se han insertado {cursor.rowcount} registros correctamente.")
    except Exception as e:
        print("Error al insertar los datos:", e)
    finally:
        # Cerrar conexión y cursor
        cursor.close()
        connection.close()

# values ejemplo
insertion = insert_row(('18 Regent Street', 'Soho', 5, 3, 168, False, True, 3, 'Semi-Detached', 'Central Heating', 'No Balcony', 'Industrial', 'Street', 'Wood', 'Renovated', 1881600.0, '2025-01-10 09:28:43', 1987, 11200.0))