# Microsoft Fabric Medallion Architecture Project

This repository contains a **complete, end-to-end Microsoft Fabric project** implementing the **Medallion Architecture (Bronze → Silver → Gold)** using **Data Pipelines, Lakehouse, PySpark Notebooks, and a Semantic Model (Direct Lake)**.

The project is **production-aligned**, **scalable**, and suitable for **learning, demos, interviews, and real-world Fabric implementations**.

---

## 🎯 Project Objective

* Ingest multiple CSV files from a **single GitHub repository** in one pipeline execution
* Store raw data in **Bronze (Lakehouse Files)**
* Clean and standardize data in **Silver (Delta tables)**
* Publish analytics-ready datasets in **Gold (Delta tables)**
* Expose data through a **Semantic Model** for Power BI reporting

---

## 📦 Source Datasets

All datasets are sourced from Microsoft Learning’s public GitHub repository:

```
https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/
```

| File          | Description          |
| ------------- | -------------------- |
| customers.csv | Customer master data |
| orders.csv    | Order transactions   |
| sales.csv     | Sales transactions   |
| products.csv  | Product catalog      |
| churn.csv     | Customer churn data  |

---

## 🏗️ Architecture Overview

```
GitHub CSV Files
        ↓
Fabric Data Pipeline
(ForEach + Copy Activity)
        ↓
Bronze Layer (Lakehouse Files)
        ↓
Notebook: Bronze → Silver
        ↓
Silver Layer (Delta Tables)
        ↓
Notebook: Silver → Gold
        ↓
Gold Layer (Delta Tables)
        ↓
Semantic Model (Direct Lake)
        ↓
Power BI Reports
```

---

## 🧪 Lakehouse Structure

```
Lakehouse
├── Files
│   └── Bronze
│       ├── customers.csv
│       ├── orders.csv
│       ├── sales.csv
│       ├── products.csv
│       └── churn.csv
└── Tables
    ├── silver.customers
    ├── silver.orders
    ├── silver.sales
    ├── silver.products
    ├── silver.churn
    ├── gold.customers
    ├── gold.orders
    ├── gold.products
    ├── gold.churn
    └── gold.calendar
```

---

## 🔁 Data Pipeline Configuration

### Pipeline Name

```
pl-github-medallion
```

### Pipeline Parameter

| Property | Value |
| -------- | ----- |
| Name     | Files |
| Type     | Array |

```json
[
  "customers.csv",
  "orders.csv",
  "sales.csv",
  "products.csv",
  "churn.csv"
]
```

---

### ForEach Activity

**Items Expression**

```
@pipeline().parameters.Files
```

Execution mode: **Sequential**

---

### Copy Activity (Inside ForEach)

**Source (HTTP)**

* Base URL:

```
https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/
```

* Relative URL:

```
@item()
```

**Sink (Lakehouse Files)**

* Folder:

```
Files/Bronze
```

* File name:

```
@item()
```

---

## 🥉 Bronze Layer (Raw Data)

* Stores raw CSV files exactly as received
* No transformations applied
* Acts as an immutable audit layer

---

## 🥈 Silver Layer (Clean Delta Tables)

### Notebook: `nb-bronze-to-silver`

**Responsibilities**

* Read CSV files from Bronze using OneLake paths
* Infer schema
* Remove duplicates
* Write Delta tables under the `silver` schema

### PySpark Code

```python
entities = ["customers", "orders", "sales", "products", "churn"]
dfs = {}

for i in entities:
    dfs[i] = spark.read.csv(
        f"/lakehouse/default/Files/Bronze/{i}.csv",
        header=True,
        inferSchema=True
    )
    print(f"{i}_df is created")

for entity, df in dfs.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"silver.{entity}")

    print(f"Delta table created: silver.{entity}")
```

---

## 🥇 Gold Layer (Business-Ready Data)

### Notebook: `nb-silver-to-gold`

**Responsibilities**

* Promote Silver tables to Gold
* Apply business-ready structure
* Create a calendar (date) dimension

### Gold Tables

```
gold.customers
gold.orders
gold.products
gold.churn
gold.calendar
```

### PySpark Code (Silver → Gold)

```python
spark.read.table("silver.customers") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("gold.customers")

spark.read.table("silver.orders") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("gold.orders")

spark.read.table("silver.products") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("gold.products")

spark.read.table("silver.churn") \
    .write.format("delta").mode("overwrite") \
    .saveAsTable("gold.churn")
```

---

### Calendar Dimension

```python
from pyspark.sql.functions import (
    min, max, sequence, explode, to_date, expr,
    year, month, dayofmonth, weekofyear,
    quarter, date_format, dayofweek
)

bounds = spark.read.table("silver.orders").select(
    min("OrderDate").alias("min_date"),
    max("OrderDate").alias("max_date")
).collect()[0]

calendar_df = (
    spark.createDataFrame([(bounds[0], bounds[1])], ["start_date", "end_date"])
    .select(
        explode(
            sequence(
                to_date("start_date"),
                to_date("end_date"),
                expr("INTERVAL 1 DAY")
            )
        ).alias("date")
    )
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("week_of_year", weekofyear("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("month_name", date_format("date", "MMMM")) \
    .withColumn("day_name", date_format("date", "EEEE")) \
    .withColumn("day_of_week", dayofweek("date")) \
    .withColumn("year_month", date_format("date", "yyyy-MM"))

calendar_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.calendar")
```

---

## 📊 Semantic Model

* Built on Gold tables
* Direct Lake mode enabled
* Centralized metrics for Power BI

**Included Tables**

* gold.customers
* gold.orders
* gold.products
* gold.churn
* gold.calendar

---

## ▶️ End-to-End Execution Flow

1. Trigger Fabric pipeline
2. ForEach activity copies CSV files from GitHub to Bronze
3. Bronze → Silver notebook executes
4. Silver → Gold notebook executes
5. Semantic model refresh runs
6. Power BI reports are updated

---

## ✅ Key Highlights

* Single pipeline ingestion
* Parameterized and scalable design
* Fabric best practices applied
* Clean schema naming (`silver.*`, `gold.*`)
* GitHub-ready documentation

---

## 🔮 Possible Enhancements

* Incremental loading
* Data quality validations
* Slowly Changing Dimensions (SCD)
* CI/CD with Fabric Git integration

---

## 👤 Author

**Your Name**
Microsoft Fabric – Medallion Architecture Project
