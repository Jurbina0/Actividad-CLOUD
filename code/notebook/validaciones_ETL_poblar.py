import pandas as pd
data = pd.read_csv('data/london_houses_clean.csv')
first_row = data.iloc[0]
print(first_row)
print(first_row.index)
print(first_row.keys())
values = first_row.values
print(values)
valores = [str(value) for value in values]
print(valores)
tupla = tuple(valores)
print(tupla)
valores = ["NULL" if value == "nan" else value for value in valores]
print(valores)
fila = tuple(valores)
print(fila)
n_rows = data.shape[0]
print(n_rows)

tuplas = []
for row in range(n_rows):
    fila = data.iloc[row]
    valores = [str(value) for value in fila.values]
    valores = ["NULL" if value == "nan" else value for value in valores]
    registro = tuple(valores)
    tuplas.append(registro)
try:
    file = open('data/london_houses.txt', 'w')
finally:    
    for tupla in tuplas:
        file.write(str(tupla) + '\n')
    file.close()