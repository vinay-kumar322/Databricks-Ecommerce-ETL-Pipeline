# Databricks notebook source
customers_df = spark.table("data_analyst_demo.ecommerce.bronze_customers")

orders_df = spark.table("data_analyst_demo.ecommerce.bronze_orders")

order_items_df = spark.table("data_analyst_demo.ecommerce.bronze_order_items")

product_df = spark.table("data_analyst_demo.ecommerce.bronze_products")

payments_df = spark.table("data_analyst_demo.ecommerce.bronze_order_payments")

reviews_df = spark.table("data_analyst_demo.ecommerce.bronze_order_reviews")

sellers_df = spark.table("data_analyst_demo.ecommerce.bronze_sellers")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog data_analyst_demo;
# MAGIC use schema ecommerce;

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_customers -----silver_customers

# COMMAND ----------

customers_df = spark.table("bronze_customers")

display(customers_df.limit(10))

# COMMAND ----------

customers_df.count()

# COMMAND ----------

customers_df.count(), customers_df.dropDuplicates().count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

customers_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in customers_df.columns
]).show()

# COMMAND ----------

from pyspark.sql.functions import upper

customers_df = customers_df.withColumn(
    "customer_city",
    upper(col("customer_city"))
)

# COMMAND ----------

customers_df = (
    customers_df
    .withColumnRenamed("customer_zip_code_prefix","zip_code")
    .withColumnRenamed("customer_city","city")
    .withColumnRenamed("customer_state","state")
)

# COMMAND ----------

customers_df.filter(
    col("zip_code").isNull()
).show()

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze order ---- silver_order

# COMMAND ----------

orders_df = spark.table("bronze_orders")

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

orders_df = spark.table("bronze_orders")

display(orders_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

orders_df = (
    orders_df
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_carrier_date",
        to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "order_delivered_customer_date",
        to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        to_timestamp("order_estimated_delivery_date")
    )
)

# COMMAND ----------

orders_df = orders_df.dropDuplicates()

# COMMAND ----------

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_order_item ----- silver_order_items

# COMMAND ----------

bronze_order_df = spark.table("bronze_order_items")

display(bronze_order_df.limit(10))

# COMMAND ----------

order_items_df = spark.table("bronze_order_items")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

order_items_df = order_items_df.withColumn(
    "shipping_limit_date",
    to_timestamp("shipping_limit_date")
)

display(order_items_df.limit(10))

# COMMAND ----------

order_items_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_product ----- silver_product
# MAGIC

# COMMAND ----------

product_df = spark.table("bronze_products")

display(product_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    product_df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in product_df.columns
    ])
)

# COMMAND ----------

product_df = product_df.dropna()

# COMMAND ----------

product_df = product_df.fillna({
    "product_category_name": "Unknown"
})

# COMMAND ----------

product_df = product_df.fillna({
    "product_weight_g": 0,
    "product_length_cm": 0,
    "product_height_cm": 0,
    "product_width_cm": 0
})

# COMMAND ----------

product_df = product_df.dropna(
    subset=[
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght"
    ]
)

# COMMAND ----------

product_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in product_df.columns
]).show()

# COMMAND ----------

from pyspark.sql.functions import col, sum

product_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in [
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght"
    ]
]).show()

# COMMAND ----------

product_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_payment--- silver_payment

# COMMAND ----------

payments_df = spark.table("bronze_payments")

display(payments_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum

payments_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in payments_df.columns
]).show()

# COMMAND ----------

payments_df.count()

payments_df.dropDuplicates().count()

# COMMAND ----------

payments_df = payments_df.dropDuplicates()

# COMMAND ----------

payments_df = payments_df.fillna({
    "payment_type": "Unknown",
    "payment_installments": 0,
    "payment_value": 0.0
})

# COMMAND ----------

payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.silver_payments")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_review----- silver_review

# COMMAND ----------

reviews_df_df = spark.table("bronze_reviews")

display(reviews_df_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

review_df = reviews_df.withColumn(
    "review_creation_date",
    to_timestamp("review_creation_date")
).withColumn(
    "review_answer_timestamp",
    to_timestamp("review_answer_timestamp")
)

# COMMAND ----------

review_df = spark.table("bronze_reviews")

# COMMAND ----------

display(review_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum

review_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in review_df.columns
]).show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

review_df = review_df.withColumn(
    "review_creation_date",
    to_timestamp("review_creation_date")
).withColumn(
    "review_answer_timestamp",
    to_timestamp("review_answer_timestamp")
)

# COMMAND ----------

review_df = review_df.fillna({
    "review_comment_title": "No Title",
    "review_comment_message": "No Review"
})

# COMMAND ----------

review_df = review_df.dropna(
    subset=[
        "review_id",
        "order_id",
        "review_score"
    ]
)

# COMMAND ----------

review_df.filter(col("review_score").isNull()).show()

# COMMAND ----------

review_df = spark.table("data_analyst_demo.ecommerce.bronze_reviews")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

review_df = review_df.withColumn(
    "review_creation_date",
    to_timestamp("review_creation_date")
).withColumn(
    "review_answer_timestamp",
    to_timestamp("review_answer_timestamp")
)

# COMMAND ----------

review_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.silver_reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_seller ----- silver_seller
# MAGIC

# COMMAND ----------

seller_df = spark.table("bronze_sellers")
display(seller_df.limit(10))




# COMMAND ----------

seller_df = spark.table("data_analyst_demo.ecommerce.bronze_sellers")

# COMMAND ----------

seller_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

seller_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in seller_df.columns
]).show()

# COMMAND ----------

seller_df = seller_df.dropna(
    subset=["seller_id"]
)

# COMMAND ----------

seller_df = seller_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import initcap, upper

seller_df = seller_df.withColumn(
    "seller_city",
    initcap(col("seller_city"))
).withColumn(
    "seller_state",
    upper(col("seller_state"))
)

# COMMAND ----------

seller_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.silver_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze geolocation ---- silver geolocation
# MAGIC

# COMMAND ----------

geolocation_df = spark.table("bronze_geolocation")
display(geolocation_df.limit(10))

# COMMAND ----------

geolocation_df = spark.table("data_analyst_demo.ecommerce.bronze_geolocation")

# COMMAND ----------

from pyspark.sql.functions import col, sum

geolocation_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in geolocation_df.columns
]).show()

# COMMAND ----------

geolocation_df = geolocation_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import initcap

geolocation_df = geolocation_df.withColumn(
    "geolocation_city",
    initcap(col("geolocation_city"))
)

# COMMAND ----------

from pyspark.sql.functions import upper

geolocation_df = geolocation_df.withColumn(
    "geolocation_state",
    upper(col("geolocation_state"))
)

# COMMAND ----------

geolocation_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

geolocation_df = geolocation_df.filter(
    (col("geolocation_lat").between(-90, 90)) &
    (col("geolocation_lng").between(-180, 180))
)

# COMMAND ----------

geolocation_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.silver_geolocation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_catagory_translations ----- silver_catagory_translations
# MAGIC

# COMMAND ----------

catagory_df = spark.table("data_analyst_demo.ecommerce.bronze_category_translation")

# COMMAND ----------

catagory_df = spark.table("bronze_category_translation")
display(catagory_df.limit(10))

# COMMAND ----------

category_df = spark.table("data_analyst_demo.ecommerce.bronze_category_translation")

# COMMAND ----------

category_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

category_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in category_df.columns
]).show()

# COMMAND ----------

category_df = category_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import trim

category_df = category_df.withColumn(
    "product_category_name",
    trim(col("product_category_name"))
).withColumn(
    "product_category_name_english",
    trim(col("product_category_name_english"))
)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

category_df = category_df.withColumn(
    "product_category_name_english",
    regexp_replace(
        col("product_category_name_english"),
        "_",
        " "
    )
)

# COMMAND ----------

from pyspark.sql.functions import initcap

category_df = category_df.withColumn(
    "product_category_name_english",
    initcap(
        regexp_replace(col("product_category_name_english"), "_", " ")
    )
)

# COMMAND ----------

category_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("data_analyst_demo.ecommerce.silver_category_translation")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS data_analyst_demo.ecommerce.silver_product_category_translation;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;