# 01_bronze_ingest.py
# Bronze layer: ingest CSV and standardize column names

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# In Databricks/Fabric this would point to the Lakehouse Files area
raw_path = "Files/raw/retail_sales_raw.csv"

# Read the raw CSV
df_raw = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(raw_path)
)

# Standardize column names for downstream layers
df_bronze = (
    df_raw
        .withColumnRenamed("Transaction ID", "InvoiceNo")
        .withColumnRenamed("Customer ID", "CustomerID")
        .withColumnRenamed("Date", "InvoiceDate")
        .withColumnRenamed("Price per Unit", "UnitPrice")
        .withColumnRenamed("Total Amount", "TotalAmount")
)

# Persist as Delta in the Bronze zone
df_bronze.write.mode("overwrite").format("delta").save("Tables/bronze_retail_sales")

