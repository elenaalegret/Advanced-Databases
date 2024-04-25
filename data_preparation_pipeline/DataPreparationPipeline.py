#################################
### Data Preparation Pipeline ###
#################################

## Description:

## Imports
from utils import *

## Connection to formatted database
jdbc_url = 'jdbc:duckdb:./../data/trusted_zone/barcelona_processed.db'
driver = "org.duckdb.DuckDBDriver"

# Loading the trusted datasets for explotation
##############################################
# SparkSession inicialitzation
spark = SparkSession.builder\
    .config("spark.jars", "duckdb.jar") \
    .appName("DataPreparation") \
    .getOrCreate()

# Loading airbnb data
df_airbnb = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", driver) \
  .option("query", "SELECT * FROM df_airbnb_listings") \
  .load()

# Loading airbnb data
df_locations = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", driver) \
  .option("query", "SELECT * FROM df_tripadvisor_locations") \
  .load()

# Criminal Dataset
df_criminal = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", driver) \
  .option("query", "SELECT * FROM df_criminal_dataset") \
  .load()

# Dics for visualization
colors = {
    'Gràcia': 'lightblue',
    'Sant Martí': 'green',
    'Horta-Guinardó': 'red',
    'Les Corts': 'purple', 
    'Sants-Montjuïc': 'orange',
    'Nou Barris': 'pink',
    'Sarrià-Sant Gervasi': 'cadetblue',
    'Eixample': 'beige',
    'Sant Andreu': 'lightgray',
    'Ciutat Vella': 'lightgreen'
}

location_icons = {
    'restaurant': 'cutlery',
    'attraction': 'star'
}
