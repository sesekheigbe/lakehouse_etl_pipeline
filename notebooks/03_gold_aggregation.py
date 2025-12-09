# 03_gold_aggregation.py
# Gold layer: customer-level sales aggregates

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col

spark = SparkSession.builder.getOrCreate()

# Load Silver table
silver = spark.read.format("delta").load("Tables/silver_retail_sales")

# Aggregate metrics at customer level
gold_sales_by_customer = (
    silver
        .groupBy("CustomerID")
        .agg(
            _sum("Quantity").alias("total_quantity"),
            _sum("TotalAmount").alias("total_revenue"),
            countDistinct("InvoiceNo").alias("num_orders")
        )
)

# Save Gold table
gold_sales_by_customer.write.mode("overwrite").format("delta").save("Tables/gold_sales_by_customer")

