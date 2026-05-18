# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer - Business Aggregations
# MAGIC Revenue metrics, customer analytics, product performance.

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, countDistinct,
    round as spark_round, current_timestamp, when, lit, max as spark_max, min as spark_min
)

# COMMAND ----------

SILVER_PATH = "/mnt/datalake/silver/transactions_clean"
GOLD_REVENUE_PATH = "/mnt/datalake/gold/daily_revenue"
GOLD_PRODUCT_PATH = "/mnt/datalake/gold/product_performance"
GOLD_CUSTOMER_PATH = "/mnt/datalake/gold/customer_ltv"

silver_df = spark.read.format("delta").load(SILVER_PATH).filter(col("dq_status") == "VALID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Revenue by Country & Category

# COMMAND ----------

daily_revenue = (
    silver_df
    .groupBy("order_date_key", "country_clean", "category")
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        count("*").alias("order_count"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("calculated_at", current_timestamp())
)

daily_revenue.write.format("delta").mode("overwrite").partitionBy("order_date_key").save(GOLD_REVENUE_PATH)
print(f"Daily revenue records: {daily_revenue.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance

# COMMAND ----------

product_perf = (
    silver_df
    .groupBy("product_id", "product_name", "category")
    .agg(
        count("*").alias("times_sold"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_sale_value"),
        countDistinct("customer_id").alias("unique_buyers"),
        spark_sum(when(col("is_returned") == True, 1).otherwise(0)).alias("return_count"),
    )
    .withColumn("return_rate", spark_round(col("return_count") / col("times_sold") * 100, 2))
    .withColumn("rank_by_revenue", spark_round(col("total_revenue"), 0))
    .orderBy(col("total_revenue").desc())
)

product_perf.write.format("delta").mode("overwrite").save(GOLD_PRODUCT_PATH)
product_perf.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Lifetime Value (CLV)

# COMMAND ----------

customer_ltv = (
    silver_df
    .groupBy("customer_id", "country_clean")
    .agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("total_amount"), 2).alias("lifetime_value"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        spark_min("order_timestamp").alias("first_order"),
        spark_max("order_timestamp").alias("last_order"),
        countDistinct("category").alias("categories_bought"),
    )
    .withColumn(
        "customer_segment",
        when(col("lifetime_value") > 5000, lit("VIP"))
        .when(col("lifetime_value") > 1000, lit("Regular"))
        .when(col("lifetime_value") > 200, lit("Occasional"))
        .otherwise(lit("One-time"))
    )
    .orderBy(col("lifetime_value").desc())
)

customer_ltv.write.format("delta").mode("overwrite").save(GOLD_CUSTOMER_PATH)
print("Customer segments:")
customer_ltv.groupBy("customer_segment").count().show()
