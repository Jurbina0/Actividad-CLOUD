import pandas as pd

data = pd.read_csv('data/london_houses_clean.csv')

def format_sql_value(value):
    """
    Formatea un valor para ser usado en SQL:
    - Strings se encierran entre comillas simples.
    - Nulos se convierten en 'NULL'.
    - Booleanos se convierten en 0 (False) o 1 (True).
    - Otros valores se mantienen como están.
    """
    if pd.isnull(value):  # Detecta valores nulos
        return "NULL"
    elif isinstance(value, str):  # Strings con comillas simples
        return f"'{value}'"
    elif isinstance(value, bool):  # Booleanos a 0/1
        return "1" if value else "0"
    else:
        return str(value)

def values_to_list(data):
    """
    Convierte un DataFrame en una lista de tuplas con formato SQL.
    Cada fila del DataFrame se convierte en una tupla lista para INSERT.
    """
    n_rows = data.shape[0]
    tuplas = []
    for row in range(n_rows):
        fila = data.iloc[row]
        valores = [format_sql_value(value) for value in fila.values]  # Formatea cada valor
        valores.insert(0, str(row + 1))  # Agrega el ID como columna extra (id_vivienda)
        registro = f"({', '.join(valores)})"
        tuplas.append(registro)
    return tuplas

# Convertir datos a formato SQL
tuplas = values_to_list(data)

try:
    with open('data/london_houses.txt', 'w') as file:
        for index, tupla in enumerate(tuplas):
            if index == len(tuplas) - 1:
                file.write(tupla + ";")  # Última línea con punto y coma
            else:
                file.write(tupla + ",\n")  # Otras líneas con coma
except Exception as e:
    print(f"Error al escribir el archivo: {e}")