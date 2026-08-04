# 🛒 Brazilian E-Commerce ETL Pipeline using Databricks

## 📌 Project Overview

This project demonstrates an end-to-end Data Engineering pipeline built using **Databricks Free Edition**, **PySpark**, **SQL**, and **Delta Lake**. The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw Brazilian E-Commerce data into business-ready datasets and interactive dashboards.

The project showcases data ingestion, transformation, cleaning, analytics, and visualization using modern data engineering practices.

---

## 🚀 Project Architecture

```
Raw Data (CSV Files)
        │
        ▼
Bronze Layer (Raw Data Ingestion)
        │
        ▼
Silver Layer (Data Cleaning & Transformation)
        │
        ▼
Gold Layer (Business Ready Tables)
        │
        ▼
Databricks SQL Dashboard
```

---

# 🛠️ Tech Stack

- Databricks Free Edition
- Apache Spark (PySpark)
- Spark SQL
- Delta Lake
- Python
- SQL
- Git
- GitHub

---

# 📂 Project Structure

```
Databricks-Ecommerce-ETL-Pipeline/
│
├── notebooks/
│   ├── 01_project_setup.py
│   ├── 02_Bronze_layer.py
│   ├── 03_Silver_layer.py
│   └── 04_Gold_layer.py
│
├── screenshots/
│   ├── Dashboard.png
│   ├── Bronze_Layer.png
│   ├── Silver_Layer.png
│   └── Gold_Layer.png
│
└── README.md
```

---

# 📊 Dataset

**Brazilian E-Commerce Public Dataset by Olist**

The dataset contains information about:

- Customers
- Orders
- Products
- Sellers
- Payments
- Order Items
- Product Categories

---

# 🏗️ Medallion Architecture

## 🥉 Bronze Layer

Raw CSV files are ingested into Delta tables without modification.

### Tasks Performed

- Read CSV files
- Infer schema
- Store raw data in Delta tables
- Preserve original data

---

## 🥈 Silver Layer

The Silver layer cleans and transforms the raw data.

### Transformations

- Removed duplicates
- Handled null values
- Standardized column names
- Corrected data types
- Filtered invalid records
- Created cleaned Delta tables

---

## 🥇 Gold Layer

The Gold layer creates business-ready analytical tables.

### Business Tables

- gold_sales
- gold_monthly_sales
- gold_payment_summary
- gold_review_summary

These tables are optimized for reporting and dashboard creation.

---

# 📈 Dashboard KPIs

The dashboard provides important business insights such as:

- Total Revenue
- Total Orders
- Total Customers
- Total Sellers
- Average Order Value
- Monthly Sales Trend
- Payment Type Distribution
- Top Product Categories
- Top Selling States

---

# 📷 Project Screenshots

## Dashboard

> Add your dashboard screenshot here

```
screenshots/Dashboard.png
```

## Bronze Layer

```
screenshots/Bronze_Layer.png
```

## Silver Layer

```
screenshots/Silver_Layer.png
```

## Gold Layer

```
screenshots/Gold_Layer.png
```

---

# 📌 Key Features

✅ End-to-End ETL Pipeline

✅ Medallion Architecture

✅ Delta Lake Tables

✅ Data Cleaning & Transformation

✅ SQL Analytics

✅ Interactive Dashboard

✅ Business KPI Reporting

---

# 📚 Skills Demonstrated

- Data Engineering
- ETL Pipeline Development
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Data Modeling
- Data Transformation
- Dashboard Development
- Git & GitHub

---

# 🎯 Business Insights

This project enables businesses to:

- Monitor revenue performance
- Analyze customer purchasing behavior
- Understand payment preferences
- Track seller performance
- Identify top-performing product categories
- Support data-driven decision making

---



💼 LinkedIn: https://www.linkedin.com/in/your-linkedin-profile

🐙 GitHub: https://github.com/vinay-kumar322

---

## ⭐ If you found this project useful, please consider giving it a Star!