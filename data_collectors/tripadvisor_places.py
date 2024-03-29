import pandas as pd
import requests
import ast
import time

airbnb = pd.read_parquet("./../data/landing_zone/airbnb_listings.parquet")

total_results = []
count = 0

# Buscar Restaurantes
for i, info in airbnb.sample(n=150).iterrows():

    url = f'https://api.content.tripadvisor.com/api/v1/location/nearby_search?latLong={info["geolocation.lat"]}%2C%20{info["geolocation.lon"]}&key=87F2BA22551E40B69B57254881723D6E&category=restaurants&radius=10&radiusUnit=km&language=en"'

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        
        print(count)

        response = response.json()

        db = pd.json_normalize(response['data'])
        db['type'] = 'restaurant'  # Añade la columna 'type' con el valor 'restaurant'
        db['district'] = info['neighbourhood_group_cleansed']
        total_results.append(db)

        count += 1
        
    #Per asseguar-se de no accedir el límit per segon de TripAdvisor
    time.sleep(0.2)

# Buscar Atracciones
for i, info in airbnb.sample(n=150).iterrows():

    url = f'https://api.content.tripadvisor.com/api/v1/location/nearby_search?latLong={info["geolocation.lat"]}%2C%20{info["geolocation.lon"]}&key=87F2BA22551E40B69B57254881723D6E&category=attractions&radius=10&radiusUnit=km&language=en"'

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        
        print(count)

        response = response.json()

        db = pd.json_normalize(response['data'])
        db['type'] = 'attraction'  # Añade la columna 'type' con el valor 'attraction'
        db['district'] = info['neighbourhood_group_cleansed']

        total_results.append(db)

        count += 1
    #Per asseguar-se de no accedir el límit per segon de TripAdvisor
    time.sleep(0.2)

tripadvisor_locations = pd.concat(total_results, ignore_index=True)

print(f'Total de filas antes de eliminar duplicados: {tripadvisor_locations.shape[0]}')

tripadvisor_locations = tripadvisor_locations.drop_duplicates(subset='location_id', keep='first')

print(f'Total de filas después de eliminar duplicados: {tripadvisor_locations.shape[0]}')

tripadvisor_locations.to_parquet('./../data/landing_zone/tripadvisor_locations.parquet')
