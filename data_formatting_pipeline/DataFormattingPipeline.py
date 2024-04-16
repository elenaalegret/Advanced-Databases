###                           ###
### Data Formatting Pipeline ###
###                           ###


# Import
import pandas as pd
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, length, lit



con = duckdb.connect(database='./../data/formatted_zone/barcelona.db', read_only=False)
con.close()

print('Iniciant Spark')
spark = SparkSession.builder\
    .config("spark.jars", "duckdb_jdbc-0.10.1.jar") \
    .getOrCreate()


## Prepare the dataset for DuckDB framework

print('Iniciant Parquets')

# Load .parquet database to Pandas Dataframe 
df_airbnb_listings = spark.read.parquet('./../data/landing_zone/airbnb_listings.parquet')
df_criminal_dataset = spark.read.parquet('./../data/landing_zone/criminal_dataset.parquet')
df_tripadvisor_locations = spark.read.parquet('./../data/landing_zone/tripadvisor_locations.parquet')
df_tripadvisor_reviews = spark.read.parquet('./../data/landing_zone/tripadvisor_reviews.parquet')





print('Escrivint 1')

jdbc_url = 'jdbc:duckdb:./../data/formatted_zone/barcelona.db'
driver = "org.duckdb.DuckDBDriver"

df_airbnb_listings.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "df_airbnb_listings") \
    .option("driver", driver) \
    .save()

df_criminal_dataset.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "df_criminal_dataset") \
    .option("driver", driver) \
    .save()

df_tripadvisor_locations.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "df_tripadvisor_locations") \
    .option("driver", driver) \
    .save()

df_tripadvisor_reviews.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "df_tripadvisor_reviews") \
    .option("driver", driver) \
    .save()

spark.stop()


