
# %%
import pandas as pd
import uuid

data = pd.read_csv('data\london_houses.csv')

sample = data.sample(5)
print(sample)
# Generamos universal unique identifiers para cada vivienda
uuids = [uuid.uuid4() for _ in range(5)]
print(uuids)
sample['uuid'] = uuids
print(sample)
new_data = pd.DataFrame()
for col in data.columns:
    sampled_col = data[col].sample(5, random_state=42).reset_index(drop=True)
    new_data[col] = sampled_col
new_data['uuid'] = [uuid.uuid4() for _ in range(5)]
print(new_data.head())
print(new_data)
print(new_data.dtypes)

