#################################
###   Data Quality Pipeline   ###
#################################

# Objective: Assess the data quality and apply data cleaning processes, storing the cleaned data in the Trusted Zone

## Imports
import duckdb

## Connect to the formatted database for Barcelona
formatted_db_path = './../data/formatted_zone/barcelona.db'
con = duckdb.connect(database=formatted_db_path, read_only=False)

## Start the data cleansing process for each table

