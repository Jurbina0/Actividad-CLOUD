import pandas as pd
from datetime import datetime
data = pd.read_csv('data/london_houses.csv')
# creamos una copia del data frame que será la que modifiquemos
df = data.copy()
# nombres de las columnas
print(df.columns)
# añadimos una columna de índices que actuará en la base de datos como principal key
# la movemos a la primera posición
df.insert(0, 'id_vivienda', df.index)
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
# visualizamos las primeras filas
print(df.head())
# recordamos los nombres de las columnas acuales
print(df.columns)
# comprobamos si hay valor nulo en dormitorio
print(df['n_dormitorios'].isna().sum())
# miramos los valores únicos de dormitorios
print(df['n_dormitorios'].unique())
# repetimos con baños
def check_unique_values(df, colname):
    print('nombre columna:', colname)
    print('n valores nulos:', df[colname].isna().sum())
    print('valores únicos: ', df[colname].unique())
    return None
check_unique_values(df, 'n_banos')
# chequemos los booleanos y el que comprueba el tipo terraza
check_unique_values(df, 'hay_jardin')
check_unique_values(df, 'hay_garaje')
# cambiamos Yes y No a los valores booleanos True y False
print(df['hay_jardin'][1:5])
df['hay_jardin'] = df['hay_jardin'].map({
    'Yes': True,
    'No': False
})
print(df['hay_jardin'][1:5])
# repetimos para garaje
print(df['hay_garaje'][1:5])
df['hay_garaje'] = df['hay_garaje'].map({
    'Yes': True,
    'No': False
})
print(df['hay_garaje'][1:5])
# chequeamos y convertimos a categorías los que empiezan por tipo
# tipo hay terraza
check_unique_values(df, 'tipo_hay_terraza')
# convertimos a categorías las columnas que empiezan por tipo
df['tipo_hay_terraza'] = df['tipo_hay_terraza'].astype('category')
# visualizamos los que han quedado como category
print(df.select_dtypes('category').value_counts())
# en una función
def convert_to_category(df, colname):
    print('nombre columna:', colname)
    df[colname] = df[colname].astype('category')
    print(df[colname].value_counts())
    return None
convert_to_category(df, 'tipo_vivienda')
# otras con tipo
check_unique_values(df, 'tipo_calefaccion')
convert_to_category(df, 'tipo_calefaccion')
convert_to_category(df, 'tipo_decorado')
convert_to_category(df, 'tipo_vistas')
convert_to_category(df, 'tipo_materiales')
convert_to_category(df, 'estado_vivienda')
# dinero a numérico
df['precio_pounds'] = df['precio_pounds'].astype('float')
# addicionamos la fecha de creación
df['fecha_creacion'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# addicionamos dos columnas vacías
df['fecha_modificacion'] = None
df['fecha_baja'] = None

# comprobamos que no hay valores extremos en los numéricos
print(df.describe())

# en global
print(df.isnull().sum())
print(df.dtypes)

# añadimos la columna para el año de construcción
df['ano_construccion'] = datetime.today().year
# restamos la edad que tiene el edificio para saber el año en que fue construido
df['ano_construccion'] = df['ano_construccion'] - df['edad_vivienda']
# comprobamos
print(df.tail())
# eliminamos la columna de edad de la vivienda
df.drop('edad_vivienda', axis=1, inplace=True)
# comprobamos
print(df.tail())
# miramos valores de los metros cuadrados y el precio
print(df[['precio_pounds','tamano']])
# calculamos precio por metros cuadrados
df['precio_metro_cuadrado'] = df['precio_pounds']/df['tamano']
# comprobamos
print(df[['precio_pounds','tamano','precio_metro_cuadrado']])
# redondeamos a un decimal por precio round
df['precio_metro_cuadrado'] = df['precio_metro_cuadrado'].round(1)
# comprobamos
print(df[['precio_pounds','tamano','precio_metro_cuadrado']])

# guardamos el dataframe en un csv
df.to_csv('data/london_houses_clean.csv', index=False)

# podemos añadir métricas con columnas con el precio en euros, USD
# podemos añadir una columna con año de construcción
# podemos añadir una tabla dimensión conforme con tiempo donde ponemos días, meses, años