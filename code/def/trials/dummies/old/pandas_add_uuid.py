# Creamos valores dummies a partir de los datos originales
import pandas as pd
import uuid

data = pd.read_csv('data\london_houses.csv')
n_data = len(data)
data['uuid'] = [uuid.uuid4() for _ in range(n_data)]
data.to_csv('data\london_houses_uuid.csv', index=False)
print("Se ha añadido una columna con UUIDs a los datos originales.")