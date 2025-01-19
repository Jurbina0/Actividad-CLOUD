# Add UUIDs to the dataset
import uuid
import pandas as pd

# Read the original dataset
data = pd.read_csv('data\\london_houses.csv')

# Generate unique identifiers (UUIDs) for all rows
data['code_vivienda'] = [uuid.uuid4() for _ in range(len(data))]

# Save the dataset with UUIDs to a CSV file
data.to_csv('data\\london_houses_uuid.csv', index=False)

print("The original data has been prepared with UUIDs.")