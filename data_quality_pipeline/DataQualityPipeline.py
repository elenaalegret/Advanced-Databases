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

### Criminal Dataset: 
# Remove duplicates
con.execute("""
    CREATE TABLE IF NOT EXISTS df_criminal_dataset_clean AS
    SELECT DISTINCT *
    FROM df_criminal_dataset
""")

# 

# You can add more cleaning processes for other tables
# Close the connection to the formatted database
con.close()




def move_to_trusted_zone():
    """
    Objective: Move cleaned data from the formatted zone to the trusted zone.
    """
    # Connect to the formatted database for Barcelona
    formatted_db_path = './../data/formatted_zone/barcelona.db'
    con_formatted = duckdb.connect(database=formatted_db_path, read_only=True)
    
    # Connect to the trusted database for Barcelona
    trusted_db_path = './../data/trusted_zone/barcelona.db'
    con_trusted = duckdb.connect(database=trusted_db_path, read_only=False)
    
    try:
        # Move cleaned data to the trusted zone
        
        # Example:
        # Move df_airbnb_listings_clean table to the trusted zone
        con_trusted.execute("""
            CREATE TABLE IF NOT EXISTS df_airbnb_listings_clean AS
            SELECT *
            FROM df_airbnb_listings_clean
        """)
        
        # You can add more tables to move to the trusted zone
        
    finally:
        # Close the connections
        con_formatted.close()
        con_trusted.close()

# Call the function to assess data quality and apply data cleaning processes
assess_data_quality_and_cleanse()

# Move cleaned data to the trusted zone
move_to_trusted_zone()
