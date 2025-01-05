# Script que crea las tablas de la base de datos
USE inmobiliaria;

# Creamos la tabla usuarios
CREATE TABLE IF NOT EXISTS usuarios (
    id_usuario INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255),
    apellido1 VARCHAR(255),
    apellido2 VARCHAR(255) NULL,  
    email VARCHAR(255),
    recibir_emails BOOLEAN,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,  
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  
    fecha_baja DATETIME NULL  
);

# Creamos la tabla viviendas
CREATE TABLE IF NOT EXISTS viviendas (
    id_vivienda INT AUTO_INCREMENT PRIMARY KEY,
    direccion VARCHAR(255),
    vecindario VARCHAR(255),
    n_dormitorios INT,
    n_banos INT,
    tamano INT,
    ano_construccion INT,
    hay_jardin BOOLEAN,
    hay_garaje BOOLEAN,
    n_plantas INT,
    tipo_vivienda VARCHAR(50),
    tipo_calefaccion VARCHAR(50),
    tipo_hay_terraza VARCHAR(50) NULL,
    tipo_decorado VARCHAR(50) NULL,
    tipo_vistas VARCHAR(50) NULL,
    tipo_materiales VARCHAR(50),
    estado_vivienda VARCHAR(50),
    precio_pounds FLOAT,
    precio_metro_cuadrado FLOAT,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    fecha_baja DATETIME NULL
);

# Creamos la tabla viviendas_favoritas
CREATE TABLE IF NOT EXISTS viviendas_favoritas (
    id_favorita INT AUTO_INCREMENT PRIMARY KEY,
    id_usuario INT,
    id_vivienda INT,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    fecha_baja DATETIME NULL,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_vivienda) REFERENCES viviendas(id_vivienda) ON DELETE CASCADE ON UPDATE CASCADE
);


# Creamos la tabla historico_precios
CREATE TABLE IF NOT EXISTS historico_precios (
    id_precio INT AUTO_INCREMENT PRIMARY KEY,
    id_vivienda INT,
    precio_pounds FLOAT,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    fecha_baja DATETIME NULL,
    FOREIGN KEY (id_vivienda) REFERENCES viviendas(id_vivienda) ON DELETE CASCADE ON UPDATE CASCADE
);