# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG data_analyst_demo;
# MAGIC USE SCHEMA ecommerce;

# COMMAND ----------

customers = spark.table("silver_customers")
orders = spark.table("silver_orders")
order_items = spark.table("silver_order_items")
products = spark.table("silver_product")
payments = spark.table("silver_payments")
reviews = spark.table("silver_reviews")
sellers = spark.table("silver_sellers")
geolocation = spark.table("silver_geolocation")
category = spark.table("silver_category_translation")




# COMMAND ----------

orders = spark.table("data_analyst_demo.ecommerce.silver_orders")
customers = spark.table("data_analyst_demo.ecommerce.silver_customers")
order_items = spark.table("data_analyst_demo.ecommerce.silver_order_items")
products = spark.table("data_analyst_demo.ecommerce.silver_product")
payments = spark.table("data_analyst_demo.ecommerce.silver_payments")
sellers = spark.table("data_analyst_demo.ecommerce.silver_sellers")

# COMMAND ----------

orders = spark.table("silver_orders")
customers = spark.table("silver_customers")
order_items = spark.table("silver_order_items")
products = spark.table("silver_product")
payments = spark.table("silver_order_payments")
sellers = spark.table("silver_sellers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN data_analyst_demo.ecommerce;

# COMMAND ----------

order_payments = spark.table("data_analyst_demo.ecommerce.silver_order_payments")
products = spark.table("data_analyst_demo.ecommerce.silver_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_sales

# COMMAND ----------

sales_df = (
    orders
    .join(customers, "customer_id", "left")
    .join(order_items, "order_id", "left")
    .join(products, "product_id", "left")
    .join(payments, "order_id", "left")
    .join(sellers, "seller_id", "left")
)

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.gold_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_product_summary

# COMMAND ----------

from pyspark.sql.functions import sum

product_summary = (
    sales_df
    .groupBy("product_id")
    .agg(
        sum("payment_value").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)
product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.gold_product_summary")

# COMMAND ----------

from pyspark.sql.functions import sum

gold_product_summary = (
    order_items_df
    .join(products_df, "product_id", "left")
    .groupBy("product_category_name")
    .agg(
        sum("price").alias("total_revenue")
    )
)
gold_product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_product_summary")

# COMMAND ----------

display(spark.table("data_analyst_demo.ecommerce.gold_product_summary"))

# COMMAND ----------

gold_product_summary.printSchema()

# COMMAND ----------

gold_product_summary = spark.table(
    "data_analyst_demo.ecommerce.gold_product_summary"
)

display(gold_product_summary)

# COMMAND ----------

order_items_df = spark.table("data_analyst_demo.ecommerce.silver_order_items")

products_df = spark.table("data_analyst_demo.ecommerce.silver_product")

# COMMAND ----------

display(products_df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_product_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_seller_summary

# COMMAND ----------

from pyspark.sql.functions import sum, desc

gold_seller_summary = (
    order_items_df
    .join(sellers_df, "seller_id", "left")
    .groupBy("seller_id")
    .agg(
        sum("price").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(10)
)

display(gold_seller_summary)

# COMMAND ----------

gold_seller_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.gold_seller_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_order_status_summary AS
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     ROUND(
# MAGIC         COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentage
# MAGIC FROM gold_sales
# MAGIC GROUP BY order_status;

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS data_analyst_demo.ecommerce.gold_seller_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_state_summary

# COMMAND ----------

state_summary = (
    sales_df
    .groupBy("state")
    .agg(
        sum("payment_value").alias("state_revenue")
    )
    .orderBy("state_revenue", ascending=False)
)
state_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_state_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_payment_summary

# COMMAND ----------

from pyspark.sql.functions import count

payment_summary = (
    payments
    .groupBy("payment_type")
    .agg(
        count("*").alias("number_of_payments"),
        sum("payment_value").alias("total_amount")
    )
)
payment_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_payment_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_review_summary

# COMMAND ----------

from pyspark.sql.functions import avg

review_summary = (
    reviews
    .groupBy("review_score")
    .agg(
        count("*").alias("number_of_reviews")
    )
    .orderBy("review_score")
)
review_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_review_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_monthly_sales

# COMMAND ----------

from pyspark.sql.functions import month, year

monthly_sales = (
    sales_df
    .groupBy(
        year("order_purchase_timestamp").alias("year"),
        month("order_purchase_timestamp").alias("month")
    )
    .agg(
        sum("payment_value").alias("monthly_revenue")
    )
    .orderBy("year", "month")
)
monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_monthly_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS data_analyst_demo.ecommerce.gold_product_summary;

# COMMAND ----------

from pyspark.sql.functions import sum

gold_product_summary = (
    order_items_df
    .join(products_df, "product_id", "left")
    .groupBy("product_category_name")
    .agg(
        sum("price").alias("total_revenue")
    )
)

gold_product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.gold_product_summary")

# COMMAND ----------

spark.table("data_analyst_demo.ecommerce.gold_product_summary").printSchema()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS data_analyst_demo.ecommerce.gold_product_summary")

# COMMAND ----------

sellers_df = spark.table("data_analyst_demo.ecommerce.silver_sellers")

products_df = spark.table("data_analyst_demo.ecommerce.silver_product")

# COMMAND ----------

from pyspark.sql.functions import sum, desc

gold_product_summary = (
    order_items_df
    .join(products_df, "product_id", "left")
    .groupBy("product_category_name")
    .agg(
        sum("price").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
    .limit(10)
)

display(gold_product_summary)

# COMMAND ----------

gold_product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.gold_product_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Total_Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ROUND(SUM(payment_value)/1000000, 2) AS Total_Revenue_Million
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2,Total_orders
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(DISTINCT order_id) AS Total_Orders
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3,Total_Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(DISTINCT customer_unique_id) AS Total_Customers
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4,Average order value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ROUND(
# MAGIC SUM(payment_value) /
# MAGIC COUNT(DISTINCT order_id),
# MAGIC 2
# MAGIC ) AS Avg_Order_Value
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5, Total sallers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(DISTINCT seller_id) AS Total_Sellers
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLE EXTENDED IN data_analyst_demo.ecommerce LIKE 'gold_sales';