# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, length, lit

spark = SparkSession.builder.appName("DataFormattingPipeline").getOrCreate()

# Load df into Spark df
criminal_dataset = spark.read.parquet('../data/landing_zone/criminal_dataset.parquet').filter(col("municipi") == "Barcelona")
airbnb_dataset = spark.read.parquet('../data/landing_zone/airbnb_listings.parquet')

# Homogeneized the dfs into a common data format
#   |--> [rea_b_sica_policial_abp] Remove the "ABP " prefix and normalize "CIUTAT VELLA" to uppercase -- match w criminal_dataset -- neighbourhood_group_cleansed
criminal_dataset = criminal_dataset.withColumn("rea_b_sica_policial_abp", upper(col("rea_b_sica_policial_abp").substr(lit(5), length(col("rea_b_sica_policial_abp")))))

#   |--> [rea_b_sica_policial_abp] Remove undesired rows
criminal_dataset = criminal_dataset.filter(~col("rea_b_sica_policial_abp").isin(["AT D'INFORMACIÓ RPMB", "FET FORA DE CATALUNYA", "BARCELONA", "FORA DE CATALUNYA"]))
#   |--> [neighbourhood_group_cleansed] same type of data and format as area_basica_policial
airbnb_dataset = airbnb_dataset.withColumn("neighbourhood_group_cleansed", upper(col("neighbourhood_group_cleansed")))

# Comprovació -- Tenim els mateixos barris amb el mateix format als dos df
criminal_dataset.select("rea_b_sica_policial_abp").distinct().show()
airbnb_dataset.select("neighbourhood_group_cleansed").distinct().show()

#   |--> Rename the cols to neighborhood_formatted
criminal_dataset = criminal_dataset.withColumnRenamed("area_basica_policial_abp", "neighborhood_formatted")
airbnb_dataset = airbnb_dataset.withColumnRenamed("neighbourhood_group_cleansed", "neighborhood_formatted")

criminal_dataset.write.parquet("../data/formatted_zone/criminal_dataset.parquet")
airbnb_dataset.write.parquet("../data/formatted_zone/airbnb_dataset.parquet")
