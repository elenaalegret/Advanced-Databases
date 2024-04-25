#### BDA - GIA 


<img src="data_preparation_pipeline/images/logo.png" alt="BCN Map4Tourism Logo" width="150">

# Advanced-Databases 

### Elena Alegret, Sergi Tomàs and Júlia Orteu
## Authors
- Elena Alegret.
- Sergi Tomàs.
- Júlia Orteu

## Project Description
This project focuses on collecting and formatting data from diverse sources, specifically targeting Airbnb listings and criminal activity datasets. Our goal is to facilitate data analysis and insights by preparing and organizing data efficiently. The repository includes raw data collection scripts, metadata documentation, and a data formatting pipeline.

Our developed tool, named **BCN Map4Tourism**, features a visually appealing map interface that seamlessly integrates all the data in the study and is designed for tourists visiting Barcelona.


## BCN Map4Tourism Application
To run the application, navigate to the directory where the `app.py` file is located (`~/Advanced-Databases/data_preparation_pipeline` in this case) and execute the following command in your terminal:

```
streamlit run app.py
```

This command will start the Streamlit application defined in the `app.py` file. You can then access the application through your web browser by following the URL provided in the terminal output (typically `http://localhost:8501`).

## Directory Structure
```
.
├── README.md
├── data
│   ├── formatted_zone
│   │   ├── airbnb_dataset.parquet
│   │   └── criminal_dataset.parquet
│   │       
│   └── landing_zone
│       ├── airbnb_listings.parquet
│       ├── criminal_dataset.parquet
│   ├── formatted_zone
│   │   ├── barcelona.db
│   │   └── barcelona_processed.db
│   ├── landing_zone
│   │   ├── airbnb_listings.parquet
│   │   ├── criminal_dataset.parquet
│   │   ├── tripadvisor_locations.parquet
│   │   └── tripadvisor_reviews.parquet
│   └── trusted_zone
│       └── barcelona_processed.db
├── data_collectors
│   ├── Metadata_Airbnb_Lisitings.md
│   ├── Metadata_Criminality_Barcelona.md
│   ├── airbnb_dataset.py
│   └── criminal_dataset.py
└── data_formatting_pipeline
    └── DataFormattingPipeline.py
│   ├── Metadata_Airbnb_Lisitings.md
│   ├── Metadata_Criminality_Barcelona.md
│   ├── Metadata_TripAdvisor_Locations.md
│   ├── Metadata_TripAdvisor_Reviews.md
│   ├── airnbnb_dataset.py
│   ├── criminal_dataset.py
│   ├── tripadvisor_places.py
│   └── tripadvisor_reviews.py
├── data_formatting_pipeline
│   ├── DataFormattingPipeline.py
│   └── sanity_check.py
├── data_quality_pipeline
│   ├── DataQualityPipeline.py
│   ├── coordinates_dict.pkl
│   ├── dataset_exploration.ipynb
│   ├── duckdb.jar
│   └── postgresql.jar
├── data_preparation_pipeline
│   ├── DataPreparationPipeline.py
│   ├── app.py
│   ├── duckdb.jar
│   ├── images
│   │   └── logo.png
│   └── postgresql.jar
└── requirements.txt
```

### data
- **landing_zone**: Contains raw data files as they are collected from the data sources.
  - `airbnb_listings.parquet`: Raw data of Airbnb listings.
  - `criminal_dataset.parquet`: Raw criminal activity data.
- **formatted_zone**: Stores data after it has been processed and formatted for analysis.
  - `airbnb_dataset.parquet`: Formatted Airbnb listings data.
  - `criminal_dataset.parquet`: Formatted criminal activity data.

### data_collectors
Scripts and metadata for data collection:
- **Metadata_Airbnb_Lisitings.md**: Documentation describing the structure and details of the Airbnb listings data.
- **Metadata_Criminality_Barcelona.md**: Documentation on the criminal dataset's structure and specifics.
- **airbnb_dataset.py**: Script to collect Airbnb listings data.
- **criminal_dataset.py**: Script to collect criminal activity data.

### data_formatting_pipeline
- **DataFormattingPipeline.py**: A Python script designed to format raw data into a structured form suitable for relational analysis. It includes functions to clean, transform, and standardize data, preparing it for the analysis.
- **sanity_check.py**: Script used to perform initial checks on data to ensure its integrity before it enters the formatting pipeline.

- **DataFormattingPipeline.py**: A Python script designed to format raw data into a structured form suitable for relacional analysis.
### data_quality_pipeline
Scripts and utilities designed to assess and improve the quality of data:
- **DataQualityPipeline.py**: Python script that implements checks and balances on the data, ensuring that data quality is maintained throughout processing.
- **coordinates_dict.pkl**: A pickle file containing pre-computed coordinates for use in validation or transformation processes.
- **dataset_exploration.ipynb**: A Jupyter Notebook used for exploratory data analysis, helping to understand data distributions and potential quality issues and solves them.

### data_preparation_pipeline
This directory contains scripts and utilities used for preparing data for visualization and analytics:
- **DataPreparationPipeline.py**: Handles further data transformations, aggregation, and preparation tasks to make the data ready for use in applications or further analytics.
- **app.py**: Python script that integrates with Streamlit to provide an interactive front-end visualization of the data.
- **images/logo.png**: Logo image used in the Streamlit application for branding and UI enhancement.
- **duckdb.jar**: Jar file for DuckDB, an embedded SQL database, used for data querying within scripts.
- **postgresql.jar**: Jar file for PostgreSQL database interactions, supporting operations that require relational database capabilities.

### requirements.txt
- Contains all Python library dependencies required by the project, ensuring all data scripts run without issues. Use `pip install -r requirements.txt` to install these dependencies.

---
---

