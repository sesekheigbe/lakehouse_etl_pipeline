# 02_silver_transform.py
# Silver layer: clean and validate transactions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.getOrCreate()

# Load Bronze table
bronze = spark.read.format("delta").load("Tables/bronze_retail_sales")

# Apply data quality filters and type normalization
silver = (
    bronze
        # Drop rows missing customer id
        .filter(col("CustomerID").isNotNull())
        # Normalize date to timestamp
        .withColumn("InvoiceDate", to_timestamp("InvoiceDate"))
        # Remove non-sensical quantities/prices
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") > 0)
)

# Save cleaned data to Silver zone
silver.write.mode("overwrite").format("delta").save("Tables/silver_retail_sales")

