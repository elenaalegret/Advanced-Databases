###                       ###
### Data Quality Pipeline ###
###                       ###

# Imports
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, length, lit


# Prepare environment
spark = SparkSession.builder.appName("DataQualityPipeline").getOrCreate()
con = duckdb.connect(database=':memory:', read_only=False)

airbnb_listings_duckdb = con.execute("SELECT * FROM airbnb_listings").fetchdf()
criminal_dataset_duckdb = con.execute("SELECT * FROM criminal_dataset").fetchdf()
tripadvisor_locations_duckdb = con.execute("SELECT * FROM tripadvisor_locations").fetchdf()
tripadvisor_reviews_duckdb = con.execute("SELECT * FROM tripadvisor_reviews").fetchdf()


# Convert DataFrame DuckDB to DataFrame Spark
airbnb_listings_quality = spark.createDataFrame(airbnb_listings_duckdb)
criminal_dataset_quality = spark.createDataFrame(criminal_dataset_duckdb)
tripadvisor_locations_quality = spark.createDataFrame(tripadvisor_locations_duckdb)
tripadvisor_reviews_quality = spark.createDataFrame(tripadvisor_reviews_duckdb)


# Homogeneized the dfs into a common data format
#   |--> [rea_b_sica_policial_abp] Remove the "ABP " prefix and normalize "CIUTAT VELLA" to uppercase -- match w criminal_dataset -- neighbourhood_group_cleansed
criminal_dataset_quality = criminal_dataset_quality.withColumn("rea_b_sica_policial_abp", upper(col("rea_b_sica_policial_abp").substr(lit(5), length(col("rea_b_sica_policial_abp")))))

#   |--> [rea_b_sica_policial_abp] Remove undesired rows
criminal_dataset_quality = criminal_dataset_quality.filter(~col("rea_b_sica_policial_abp").isin(["AT D'INFORMACIÓ RPMB", "FET FORA DE CATALUNYA", "BARCELONA", "FORA DE CATALUNYA"]))
#   |--> [neighbourhood_group_cleansed] same type of data and format as area_basica_policial
airbnb_listings_quality = airbnb_listings_quality.withColumn("neighbourhood_group_cleansed", upper(col("neighbourhood_group_cleansed")))

# Comprovació -- Tenim els mateixos barris amb el mateix format als dos df
criminal_dataset_quality.select("rea_b_sica_policial_abp").distinct().show()
airbnb_listings_quality.select("neighbourhood_group_cleansed").distinct().show()

#   |--> Rename the cols to neighborhood_formatted
criminal_dataset_quality = criminal_dataset_quality.withColumnRenamed("area_basica_policial_abp", "neighborhood_formatted")
airbnb_listings_quality = airbnb_listings_quality.withColumnRenamed("neighbourhood_group_cleansed", "neighborhood_formatted")

criminal_dataset_quality.write.parquet("../data/formatted_zone/criminal_dataset_quality.parquet")
airbnb_listings_quality.write.parquet("../data/formatted_zone/airbnb_listings_quality.parquet")



# Convert Spark DataFrame TO Pandas DataFrame
df_airbnb_listings_pandas = airbnb_listings_quality.toPandas()

# Establecer una conexión a un nuevo DuckDB
con_quality = duckdb.connect(database=':memory:', read_only=False)

# Registrar el DataFrame de Pandas en el nuevo DuckDB
con_quality.register('airbnb_listings', airbnb_listings_quality)
con_quality.register('airbnb_listings', airbnb_listings_quality)
con_quality.register('airbnb_listings', airbnb_listings_quality)
con_quality.register('airbnb_listings', airbnb_listings_quality)



spark.stop()
