# Creamos valores dummies a partir de los datos originales
import pandas as pd
data = pd.read_csv('data\london_houses.csv')
new_data = pd.DataFrame()
for col in data.columns:
    # elegimos el 10% de los datos de cada columna
    sampled_col = data[col].sample(frac=0.1, random_state=42).reset_index(drop=True)
    new_data[col] = sampled_col
"""
n_values = int(len(data)*0.1)
new_data['uuid'] = [uuid.uuid4() for _ in range(n_values)]
"""
new_data.to_csv('data\london_houses2.csv', index=False)
print("Se ha creado una nueva muestra.")