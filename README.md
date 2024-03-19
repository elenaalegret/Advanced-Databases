#### BDA - GIA 

# Advanced-Databases 

### Elena Alegret, Sergi Tomàs and Júlia Orteu

This project focuses on collecting and formatting data from diverse sources, specifically targeting Airbnb listings and criminal activity datasets. Our goal is to facilitate data analysis and insights by preparing and organizing data efficiently. The repository includes raw data collection scripts, metadata documentation, and a data formatting pipeline.

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
├── data_collectors
│   ├── Metadata_Airbnb_Lisitings.md
│   ├── Metadata_Criminality_Barcelona.md
│   ├── airbnb_dataset.py
│   └── criminal_dataset.py
└── data_formatting_pipeline
    └── DataFormattingPipeline.py
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

- **DataFormattingPipeline.py**: A Python script designed to format raw data into a structured form suitable for relacional analysis.




