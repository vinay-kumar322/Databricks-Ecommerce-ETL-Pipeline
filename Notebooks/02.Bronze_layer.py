# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG data_analyst_demo;
# MAGIC USE SCHEMA ecommerce;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze_customers

# COMMAND ----------

customers_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_customers_dataset.csv")
)

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_orders

# COMMAND ----------

orders_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_orders_dataset.csv")
)
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_order_items

# COMMAND ----------

order_items_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_items_dataset.csv")
)
order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_products

# COMMAND ----------

products_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_products_dataset.csv")
)
products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_payments

# COMMAND ----------

payments_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_payments_dataset.csv")
)

payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS data_analyst_demo.ecommerce.bronze_reviews;

# COMMAND ----------

reviews_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("multiLine", True)
         .option("quote", "\"")
         .option("escape", "\"")
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_reviews_dataset.csv")
)

# COMMAND ----------

reviews_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.bronze_reviews")

# COMMAND ----------

display(reviews_df.limit(10))

# COMMAND ----------

reviews_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_reviews_dataset.csv")
)

reviews_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_sellers

# COMMAND ----------

sellers_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_sellers_dataset.csv")
)

sellers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_geolocation

# COMMAND ----------

geo_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/olist_geolocation_dataset.csv")
)

geo_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze_category_translation

# COMMAND ----------

category_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("/Volumes/data_analyst_demo/ecommerce/olist_data/product_category_name_translation.csv")
)

category_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_category_translation")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_customers;

# COMMAND ----------

show tables;