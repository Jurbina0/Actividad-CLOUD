USE inmobiliaria;

INSERT INTO viviendas (
    id_vivienda, direccion, vecindario, n_dormitorios, n_banos,
    tamano, ano_construccion, hay_jardin, hay_garaje, n_plantas,
    tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
    tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda,
    precio_pounds, precio_metro_cuadrado, fecha_creacion, fecha_modificacion, fecha_baja
) 
VALUES (
    1, '90 Piccadilly Circus', 'Kensington', 3, 3, 160, 93, FALSE, FALSE, 3, 
    'Semi-Detached', 'Gas Heating', 'No Balcony', 'Classic', 'Sea', 
    'Laminate Flooring', 'Old', 2431999.0, 23453423.0, 
    '2025-01-03 12:59:09', NULL, NULL
);
