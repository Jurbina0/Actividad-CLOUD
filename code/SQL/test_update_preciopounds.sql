USE inmobiliaria;
UPDATE viviendas SET precio_pounds = 20 WHERE id_vivienda = 4006;
SELECT * FROM inmobiliaria.historico_precios WHERE id_vivienda=4006;
SELECT precio_metro_cuadrado, precio_pounds FROM inmobiliaria.viviendas WHERE id_vivienda=4006;