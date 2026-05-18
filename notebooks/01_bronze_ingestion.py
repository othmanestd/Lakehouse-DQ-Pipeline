# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC Loads raw e-commerce CSV data into Delta Lake without transformation.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# COMMAND ----------

RAW_PATH = "/mnt/datalake/raw/ecommerce/"
BRONZE_PATH = "/mnt/datalake/bronze/transactions"

# COMMAND ----------

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("country", StringType()),
    StructField("order_date", StringType()),
    StructField("is_returned", BooleanType()),
])

# COMMAND ----------

raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(RAW_PATH)
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_batch_id", lit("batch_001"))
)

print(f"Raw records loaded: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

(
    raw_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)

print(f"Bronze table row count: {spark.read.format('delta').load(BRONZE_PATH).count()}")
