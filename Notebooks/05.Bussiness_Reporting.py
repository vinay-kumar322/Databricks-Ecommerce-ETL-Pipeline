# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG data_analyst_demo;
# MAGIC USE SCHEMA ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ROUND(SUM(payment_value),2) AS Total_Revenue
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(DISTINCT order_id) AS Total_Orders
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC COUNT(DISTINCT customer_unique_id) AS Total_Customers
# MAGIC FROM gold_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC product_id,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY product_id
# MAGIC ORDER BY Revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC seller_id,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY seller_id
# MAGIC ORDER BY Revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC state,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY state
# MAGIC ORDER BY Revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC payment_type,
# MAGIC COUNT(*) AS Total_Payments,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY Revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AVG(review_score) AS Average_Rating
# MAGIC FROM silver_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC review_score,
# MAGIC COUNT(*) AS Total_Reviews
# MAGIC FROM silver_reviews
# MAGIC GROUP BY review_score
# MAGIC ORDER BY review_score;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC YEAR(order_purchase_timestamp) AS Year,
# MAGIC MONTH(order_purchase_timestamp) AS Month,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY
# MAGIC YEAR(order_purchase_timestamp),
# MAGIC MONTH(order_purchase_timestamp)
# MAGIC ORDER BY Year,Month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC city,
# MAGIC ROUND(SUM(payment_value),2) AS Revenue
# MAGIC FROM gold_sales
# MAGIC GROUP BY city
# MAGIC ORDER BY Revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC order_status,
# MAGIC COUNT(*) AS Orders
# MAGIC FROM gold_sales
# MAGIC GROUP BY order_status
# MAGIC ORDER BY Orders DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AVG(
# MAGIC DATEDIFF(
# MAGIC order_delivered_customer_date,
# MAGIC order_purchase_timestamp
# MAGIC )
# MAGIC ) AS Avg_Delivery_Days
# MAGIC FROM silver_orders
# MAGIC WHERE order_status='delivered';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_unique_id,
# MAGIC COUNT(DISTINCT order_id) AS Total_Orders
# MAGIC FROM gold_sales
# MAGIC GROUP BY customer_unique_id
# MAGIC ORDER BY Total_Orders DESC
# MAGIC LIMIT 10;

# COMMAND ----------

