USE inmobiliaria;

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