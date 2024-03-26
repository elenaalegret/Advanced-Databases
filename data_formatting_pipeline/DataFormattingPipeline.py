###                           ###
### Data Formatting Pipeline ###
###                           ###


# Import
import pandas as pd
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, length, lit

spark = SparkSession.builder.appName("DataFormattingPipeline").getOrCreate()

## Prepare the dataset for DuckDB framework

# Load .parquet database to Pandas Dataframe 
df_airbnb_listings = spark.read.parquet('./data/landing_zone/airbnb_listings.parquet')
df_criminal_dataset = spark.read.parquet('./data/landing_zone/criminal_dataset.parquet')
df_tripadvisor_locations = spark.read.parquet('./data/landing_zone/tripadvisor_locations.parquet')
df_tripadvisor_reviews = spark.read.parquet('./data/landing_zone/tripadvisor_reviews.parquet')

# Make connection to DuckDB database
con = duckdb.connect(database=':memory:', read_only=False)

# Insert DataFrame to DuckDB
con.register('airbnb_listings', df_airbnb_listings)
con.register('criminal_dataset', df_criminal_dataset)
con.register('tripadvisor_locations', df_tripadvisor_locations)
con.register('tripadvisor_reviews', df_tripadvisor_reviews)

spark.stop()


"""
# CONSULT EXAMPLE:
query_airbnb_listings = "SELECT * FROM airbnb_listings;"

# Execute consult SQL and take results 
result_airbnb_listings = con.execute(query_airbnb_listings).fetchall()
print(result_airbnb_listings)
"""