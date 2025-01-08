import pandas as pd
data = pd.read_csv('data/london_houses_clean.csv')
def values_to_list(data):
    n_rows = data.shape[0]
    tuplas = []
    for row in range(n_rows):
        fila = data.iloc[row]
        valores = [str(value) for value in fila.values]
        valores = ["NULL" if value == "nan" else value for value in valores]
        valores[0] = str(row+1)
        registro = tuple(valores)
        tuplas.append(registro)
    return tuplas
tuplas = values_to_list(data)
try:
    file = open('data/london_houses.txt', 'w')
    for index, tupla in enumerate(tuplas):
        if index == len(tuplas) - 1:
            file.write(str(tupla) + ';' + '\n')  # Última línea con punto y coma
        else:
            file.write(str(tupla) + ',' + '\n')  # Otras líneas con coma
finally:
    file.close()