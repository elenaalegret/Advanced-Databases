import pandas as pd
import requests
import ast
import time

tripadvisor = pd.read_parquet("./../data/landing_zone/tripadvisor_locations.parquet")

total_results = []
count = 0

print(tripadvisor.shape)
# Buscar Restaurantes
for i, info in tripadvisor.iterrows():

    url = f'https://api.content.tripadvisor.com/api/v1/location/{info["location_id"]}/reviews?key=87F2BA22551E40B69B57254881723D6E&language=en&limit=80'

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        
        print(count)

        response = response.json()
        db = pd.json_normalize(response['data'])
        total_results.append(db)

        count += 1
    
    #Per asseguar-se de no accedir el límit per segon de TripAdvisor
    time.sleep(0.2)

tripadvisor_reviews = pd.concat(total_results, ignore_index=True)

tripadvisor_reviews.to_parquet('./../data/landing_zone/tripadvisor_reviews.parquet')