# Create dummy values based on the original data
import uuid
import pandas as pd

# Read the original dataset
data = pd.read_csv('data\\london_houses.csv')

# Create a new DataFrame for the sampled data
new_data = pd.DataFrame()

for col in data.columns:
    # Select 10% of the data from each column
    sampled_col = data[col].sample(frac=0.1, random_state=42).reset_index(drop=True)
    new_data[col] = sampled_col

# Calculate the number of values to sample
n_values = int(len(data) * 0.1)

# Generate unique identifiers (UUIDs) for the sampled rows
new_data['code_vivienda'] = [uuid.uuid4() for _ in range(n_values)]

# Save the new sampled dataset to a CSV file
new_data.to_csv('data\\london_houses2_uuid.csv', index=False)

print("A new sample has been created.")