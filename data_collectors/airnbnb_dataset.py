# Dataset Preparation ~ airbnb_listings

# Imports
import pandas as pd
import requests
import io


# Fem la request a la api per carregar tot el dataset
total_results = []  
start = 0
rows = 100
while start < 10000:
    # anar sumant de 100 en 100: &start=0&rows=100 ...
    # Filtem directament a Barcelona i evitant que la variable summary sigui NULL
    response = requests.get(f'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/airbnb-listings/records?where=city%20like%20%22Barcelona%22%20and%20summary%20is%20not%20null&start={start}&limit={rows}').json()
    total_count = response['total_count']  
    results_normalized = pd.json_normalize(response, 'results')
    total_results.append(results_normalized) 
    start += rows  

# Convino totes les requests en una sola
airbnb_listings = pd.concat(total_results, ignore_index=True)

airbnb_listings.to_parquet('./../data/landing_zone/airbnb_listings.parquet')
