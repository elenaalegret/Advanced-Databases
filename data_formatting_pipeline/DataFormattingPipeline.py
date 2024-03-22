
###                           ###
### Data Formatting Pipeline ###
###                           ###


# Import
import pandas as pd
import duckdb

## Prepare the dataset for DuckDB framework

# Load .parquet database to Pandas Dataframe 
df_airbnb_listings = pd.read_parquet(path='/Users/elenaalegretregalado/Desktop/Advanced-Databases/data/landing_zone/airbnb_listings.parquet')
df_criminal_dataset = pd.read_parquet(path='/Users/elenaalegretregalado/Desktop/Advanced-Databases/data/landing_zone/criminal_dataset.parquet')
#df_tripadvisor_locations = pd.read_parquet(ruta_archivo_parquet='/Users/elenaalegretregalado/Desktop/Advanced-Databases/data/landing_zone/tripadvisor_locations.parquet')
#df_tripadvisor_reviews = pd.read_parquet(ruta_archivo_parquet='/Users/elenaalegretregalado/Desktop/Advanced-Databases/data/landing_zone/tripadvisor_reviews.parquet')


# Make connection to DuckDB database
con = duckdb.connect(database=':memory:', read_only=False)

# Insert DataFrame to DuckDB
con.register('airbnb_listings', df_airbnb_listings)
con.register('criminal_dataset', df_criminal_dataset)
#con.register('tripadvisor_locations', df_tripadvisor_locations)
#con.register('tripadvisor_reviews', df_tripadvisor_reviews)



"""
# CONSULT EXAMPLE:
query_airbnb_listings = "SELECT * FROM airbnb_listings;"

# Execute consult SQL and take results 
result_airbnb_listings = con.execute(query_airbnb_listings).fetchall()
print(result_airbnb_listings)
"""