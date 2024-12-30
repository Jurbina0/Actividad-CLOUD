# Script que crea las tablas de la base de datos
USE inmobiliaria;

# Creamos la tabla usuarios
CREATE TABLE usuarios (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255),
    apellido1 VARCHAR(255),
    apellido2 VARCHAR(255) NULL,  
    email VARCHAR(255),
    consentimiento BOOLEAN,
    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,  
    fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  
    fecha_baja DATETIME NULL  
);