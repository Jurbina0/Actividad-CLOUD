import pandas as pd
data = pd.read_csv('data/london_houses_clean.csv')
y = []
for row in data.iterrows():
    index, data = row
    y.append(tuple(data.tolist()))

file = open('data/london_houses.txt', 'w')
for tuple in y:
    file.write(str(tuple) + ','+'\n')
file.close()