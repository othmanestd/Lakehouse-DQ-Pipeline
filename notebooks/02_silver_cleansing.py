# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer - Data Cleansing & Quality
# MAGIC Deduplication, DQ checks, type casting, enrichment.

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, row_number, to_timestamp, date_format,
    current_timestamp, upper, trim, regexp_replace, abs as spark_abs
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

BRONZE_PATH = "/mnt/datalake/bronze/transactions"
SILVER_PATH = "/mnt/datalake/silver/transactions_clean"
REJECTED_PATH = "/mnt/datalake/quarantine/transactions_rejected"

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze records: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Deduplication

# COMMAND ----------

dedup_window = Window.partitionBy("transaction_id").orderBy(col("_ingestion_timestamp").desc())
deduped_df = (
    bronze_df
    .withColumn("_row_num", row_number().over(dedup_window))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)
dupes_removed = bronze_df.count() - deduped_df.count()
print(f"Duplicates removed: {dupes_removed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Data Quality Checks

# COMMAND ----------

validated_df = deduped_df.withColumn(
    "dq_status",
    when(col("transaction_id").isNull(), lit("REJECTED"))
    .when(col("customer_id").isNull(), lit("REJECTED"))
    .when(col("quantity") <= 0, lit("REJECTED"))
    .when(col("total_amount") <= 0, lit("REJECTED"))
    .when(col("payment_method").isin("", None), lit("FLAGGED"))
    .when(col("country").isNull(), lit("FLAGGED"))
    .otherwise(lit("VALID"))
)

print("DQ Status Distribution:")
validated_df.groupBy("dq_status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Quarantine rejected records

# COMMAND ----------

rejected_df = validated_df.filter(col("dq_status") == "REJECTED")
rejected_df.write.format("delta").mode("append").save(REJECTED_PATH)
print(f"Quarantined {rejected_df.count()} rejected records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Enrich and write Silver

# COMMAND ----------

silver_df = (
    validated_df
    .filter(col("dq_status") != "REJECTED")
    .withColumn("order_timestamp", to_timestamp(col("order_date")))
    .withColumn("order_date_key", date_format(col("order_timestamp"), "yyyy-MM-dd"))
    .withColumn("order_month", date_format(col("order_timestamp"), "yyyy-MM"))
    .withColumn("country_clean", upper(trim(col("country"))))
    .withColumn("payment_clean", when(col("payment_method") == "", lit("unknown")).otherwise(col("payment_method")))
    .withColumn("quantity_abs", spark_abs(col("quantity")))
    .withColumn("_processed_at", current_timestamp())
    .drop("_source_file", "_batch_id", "order_date")
)

# COMMAND ----------

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("t")
        .merge(silver_df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").partitionBy("order_month").save(SILVER_PATH)

print(f"Silver table row count: {spark.read.format('delta').load(SILVER_PATH).count()}")
