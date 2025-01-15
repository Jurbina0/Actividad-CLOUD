USE inmobiliaria;
DELETE FROM viviendas WHERE id_vivienda>0;
DELETE FROM historico_precios WHERE id_vivienda>0;
-- reiniciamos el id_viviendas
TRUNCATE TABLE historico_precios;
TRUNCATE TABLE viviendas;