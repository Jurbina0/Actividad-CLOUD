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

df['tipo_hay_terraza'] = df['tipo_hay_terraza'].astype('category')
def convert_to_category(df, colname):
    df[colname] = df[colname].astype('category')
    return None
convert_to_category(df, 'tipo_vivienda')
convert_to_category(df, 'tipo_calefaccion')
convert_to_category(df, 'tipo_decorado')
convert_to_category(df, 'tipo_vistas')
convert_to_category(df, 'tipo_materiales')
convert_to_category(df, 'estado_vivienda')
df['precio_pounds'] = df['precio_pounds'].astype('float')
# addicionamos la fecha de creación
df['fecha_creacion'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
# addicionamos dos columnas vacías
df['fecha_modificacion'] = None
df['fecha_baja'] = None
# guardamos el dataframe en un csv
df.to_csv('data/london_houses_clean.csv', index=False)