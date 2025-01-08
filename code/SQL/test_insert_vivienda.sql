USE inmobiliaria;

INSERT INTO viviendas (
    id_vivienda, direccion, vecindario, n_dormitorios, n_banos,
    tamano, ano_construccion, hay_jardin, hay_garaje, n_plantas,
    tipo_vivienda, tipo_calefaccion, tipo_hay_terraza,
    tipo_decorado, tipo_vistas, tipo_materiales, estado_vivienda,
    precio_pounds, precio_metro_cuadrado, fecha_creacion, fecha_modificacion, fecha_baja
) 
VALUES ('997', 'Camden', '4', '1', '191', 'False', 'True', '2', 'Semi-Detached', 'Central Heating', 'Low-level Balcony', 'Minimalist', 'Park', 'Laminate Flooring', 'Old', '1986399.0', '2025-01-08 12:50:40', 'NULL', 'NULL', '1998', '10400.0'),
('998', 'Camden', '5', '2', '131', 'True', 'False', '2', 'Detached House', 'Underfloor Heating', 'High-level Balcony', 'Modern', 'Park', 'Laminate Flooring', 'Renovated', '1703000.0', '2025-01-08 12:50:40', 'NULL', 'NULL', '2022', '13000.0'),
('999', 'Kensington', '4', '2', '185', 'False', 'False', '1', 'Apartment', 'Electric Heating', 'No Balcony', 'Modern', 'Street', 'Wood', 'Old', '2343333.0', '2025-01-08 12:50:40', 'NULL', 'NULL', '1972', '12666.7');

