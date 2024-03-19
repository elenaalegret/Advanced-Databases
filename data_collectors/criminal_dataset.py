#### Dataset Preparation

# Imports
import pandas as pd
import requests
import io

# Datasets URL
dataset_urls = {
    'criminal_acts': 'https://analisi.transparenciacatalunya.cat/resource/y48r-ae59.json?$limit=50000000'
}

# Retrieving and Reading Criminal Dataset
criminal_dataset = requests.get(dataset_urls['criminal_acts']).content
criminal_dataset = pd.read_json(io.StringIO(criminal_dataset.decode('utf-8')))

# Load and read in .parquet format
criminal_dataset.to_parquet('./../data/landing_zone/criminal_dataset.parquet')


