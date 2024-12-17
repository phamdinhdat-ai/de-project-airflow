import os 
import pandas as pd


file_path = 'houses.json'
df = pd.read_json(file_path)
print(df.columns)
print(df.head())
df.to_csv('houses.csv') 