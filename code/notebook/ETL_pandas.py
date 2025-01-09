import pandas as pd
from datetime import datetime
data = pd.read_csv('data/london_houses.csv')
# creamos una copia del data frame que será la que modifiquemos
df = data.copy()
# añadimos una columna de índices que actuará en la base de datos como principal key
# la movemos a la primera posición
# cambiamos los nombres de las columnas y creamos una copia del dataframe
df = df.rename(columns={
    'Address': 'direccion',
    'Neighborhood': 'vecindario',
    'Bedrooms': 'n_dormitorios',
    'Bathrooms': 'n_banos',
    'Square Meters': 'tamano',
    'Building Age': 'edad_vivienda',
    'Garden': 'hay_jardin',
    'Garage': 'hay_garaje',
    'Floors': 'n_plantas',
    'Property Type': 'tipo_vivienda',
    'Heating Type': 'tipo_calefaccion',
    'Balcony': 'tipo_hay_terraza',
    'Interior Style': 'tipo_decorado',
    'View': 'tipo_vistas',
    'Materials': 'tipo_materiales',
    'Building Status': 'estado_vivienda',
    'Price (£)': 'precio_pounds'
})

df['hay_jardin'] = df['hay_jardin'].map({
    'Yes': True,
    'No': False
})

df['hay_garaje'] = df['hay_garaje'].map({
    'Yes': True,
    'No': False
})
print(df.dtypes)
# convertimos
for colname in ['tipo_hay_terraza','tipo_vivienda','tipo_calefaccion','tipo_decorado', 'tipo_vistas','tipo_materiales', 'estado_vivienda']:
    df[colname].astype('category')
df['precio_pounds'] = df['precio_pounds'].astype('float')
# addicionamos la fecha de creación
df['fecha_creacion'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
# addicionamos dos columnas vacías
df['fecha_modificacion'] = None
df['fecha_baja'] = None
# añadimos la columna para el año de construcción
df['ano_construccion'] = datetime.today().year
# restamos la edad que tiene el edificio para saber el año en que fue construido
df['ano_construccion'] = df['ano_construccion'] - df['edad_vivienda']
# eliminamos la columna de edad de la vivienda
df.drop('edad_vivienda', axis=1, inplace=True)
# calculamos precio por metros cuadrados
df['precio_metro_cuadrado'] = df['precio_pounds']/df['tamano']
# redondeamos a un decimal por precio round
df['precio_metro_cuadrado'] = df['precio_metro_cuadrado'].round(1)
# guardamos el dataframe en un csv
df.to_csv('data/london_houses_clean.csv', index=False)

"""
# cambiamos a NULL para que sea compatible con la base de datos
df['fecha_modificacion'] = df['fecha_modificacion'].map({
    None: 'NULL'
})
df['fecha_baja'] = df['fecha_baja'].map({
    None: 'NULL'
})
"""