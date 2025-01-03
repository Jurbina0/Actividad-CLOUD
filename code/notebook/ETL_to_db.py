import mysql.connector
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="PROPILENO24a",  # Sustituye por tu contraseña
    database="inmobiliaria"
)

cursor = db_connection.cursor()

# SQL para crear la tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS propiedades (
    id_vivienda INT PRIMARY KEY,
    direccion VARCHAR(255),
    vecindario VARCHAR(255),
    n_dormitorios INT,
    n_banos INT,
    tamano INT,
    edad_vivienda INT,
    hay_jardin BOOLEAN,
    hay_garaje BOOLEAN,
    n_plantas INT,
    tipo_vivienda VARCHAR(50),
    tipo_calefaccion VARCHAR(50),
    tipo_hay_terraza VARCHAR(50),
    tipo_decorado VARCHAR(50),
    tipo_vistas VARCHAR(50),
    tipo_materiales VARCHAR(50),
    estado_vivienda VARCHAR(50),
    precio_pounds FLOAT,
    fecha_creacion DATE,
    fecha_modificacion DATE,
    fecha_baja DATE
);
"""
cursor.execute(create_table_query)
db_connection.commit()

print("Tabla vivienda creada con éxito.")
