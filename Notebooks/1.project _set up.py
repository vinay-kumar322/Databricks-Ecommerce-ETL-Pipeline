# Databricks notebook source
# MAGIC %md
# MAGIC ## create project schema
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog data_analyst_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS data_analyst_demo.ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN data_analyst_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG data_analyst_demo;
# MAGIC
# MAGIC USE SCHEMA ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();
# MAGIC
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the valume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS olist_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload the files and  read it
# MAGIC

# COMMAND ----------

customers_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

customers_df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

customers_df.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in customers_df.columns
    ]
).show()

# COMMAND ----------

orders_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

display(orders_df.limit(10))

# COMMAND ----------

orders_df.printSchema();

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_item_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

display(orders_item_df.limit(10))

# COMMAND ----------

product_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)

display(product_df.limit(10))

# COMMAND ----------


payment_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)

display(payment_df.limit(10))

# COMMAND ----------


order_reviews_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True
)

display(order_reviews_df.limit(10))

# COMMAND ----------



from pyspark.sql.functions import col, sum

order_reviews_df.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in order_reviews_df.columns
    ]
).show()

# COMMAND ----------

sellers_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)

display(sellers_df.limit(10))

# COMMAND ----------

geolocation_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/olist_geolocation_dataset.csv",
    header=True,
    inferSchema=True
)

display(geolocation_df.limit(10))

# COMMAND ----------

product_category_df = spark.read.csv(
    "/Volumes/data_analyst_demo/ecommerce/olist_data/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)

display(product_category_df.limit(10))

# COMMAND ----------

# Load Silver Tables

customers = spark.table("data_analyst_demo.ecommerce.silver_customers")
orders = spark.table("data_analyst_demo.ecommerce.silver_orders")
order_items = spark.table("data_analyst_demo.ecommerce.silver_order_items")
products = spark.table("data_analyst_demo.ecommerce.silver_product")
payments = spark.table("data_analyst_demo.ecommerce.silver_order_payments")
reviews = spark.table("data_analyst_demo.ecommerce.silver_reviews")
sellers = spark.table("data_analyst_demo.ecommerce.silver_sellers")
category = spark.table("data_analyst_demo.ecommerce.silver_product_category_translation")
geolocation = spark.table("data_analyst_demo.ecommerce.silver_geolocation")