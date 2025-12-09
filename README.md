# Lakehouse ETL Pipeline (Databricks + PySpark + Delta Lake)

## Overview
This project demonstrates a complete Lakehouse ETL pipeline for a retail analytics use case. It follows the Medallion Architecture (Bronze → Silver → Gold) and showcases how raw CSV data can be ingested, cleaned, and transformed into analytics-ready datasets using PySpark and Delta Lake. This structure is similar to pipelines built in Databricks or Microsoft Fabric.

## Dataset
The dataset used in this project is a public retail sales CSV downloaded from Kaggle. The raw file is stored in:

data/raw/retail_sales_raw.csv

The source file contains the following fields:

- Transaction ID  
- Date  
- Customer ID  
- Gender  
- Age  
- Product Category  
- Quantity  
- Price per Unit  
- Total Amount  

These columns are standardized and cleaned during the Bronze and Silver stages.

## Bronze Layer
File: 01_bronze_ingest.py

The Bronze layer handles raw data ingestion and performs the following tasks:

- Load the original CSV file  
- Standardize inconsistent column names (for example, converting "Transaction ID" to "InvoiceNo")  
- Prepare the data for downstream processing  
- Save the output as a Delta table named `bronze_retail_sales`

This layer intentionally avoids heavy transformations to preserve the integrity of the raw data.

## Silver Layer
File: 02_silver_transform.py

The Silver layer focuses on data cleaning and quality improvements. It performs:

- Removal of records with missing customer identifiers  
- Conversion of the `InvoiceDate` field to a timestamp data type  
- Filtering out invalid quantities and prices  
- Preparing a clean, analysis-ready Delta table named `silver_retail_sales`

The goal is to create consistent, high-quality data for analytical processing.

## Gold Layer
File: 03_gold_aggregation.py

The Gold layer produces aggregated datasets used by analytics teams or reporting tools. In this project, the Gold layer computes the following metrics at the customer level:

- Total quantity purchased  
- Total revenue  
- Number of unique orders  

The final aggregated Delta table is saved as `gold_sales_by_customer`. This Gold dataset can be used directly for dashboards, BI tools, or machine learning feature engineering.

## Project Structure
lakehouse-etl-pipeline/
│
├── data/
│ ├── raw/ Contains the original Kaggle CSV file
│ ├── bronze/ (Empty placeholder folders for conceptual pipeline structure)
│ ├── silver/
│ └── gold/
│
├── notebooks/
│ ├── 01_bronze_ingest.py
│ ├── 02_silver_transform.py
│ └── 03_gold_aggregation.py
│
├── src/
│ └── etl_utils.py
│
└── README.md


## Technologies Used
- PySpark  
- Delta Lake  
- Databricks-style notebook workflows  
- Medallion Architecture (Bronze, Silver, Gold)

## Notes
This project is structured to reflect professional Data Engineering practices used in modern cloud environments. While the code is written in PySpark, it is not executed locally. The purpose is to demonstrate familiarity with Lakehouse design patterns, ETL workflows, and PySpark transformations.

To view this project in the context of a full portfolio, see:

https://github.com/sesekheigbe/data-engineering-portfolio
