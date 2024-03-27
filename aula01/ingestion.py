import requests
import pandas as pd


api_url = 'https://randomuser.me/api/?results=5'
response = requests.get(api_url).json()
data = response['results']
df = pd.json_normalize(data)
df.to_csv("raw.csv", sep=";", index=False)