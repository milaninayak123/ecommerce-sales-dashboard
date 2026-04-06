# Databricks notebook source
## Data Ingestion + Exploration Phase
## EDA (Exploratory Data Analysis)
## Bronze Layer = RAW DATA
df = spark.sql("SELECT * FROM online_retail_ii")
df.show(5)

# COMMAND ----------

## shows how many NULLs in each column
from pyspark.sql.functions import col,count,when
df.select([
    count(when(col(c).isNull(),c)).alias(c) for c in df.columns
]).show()

# COMMAND ----------

# Data Cleaning + Feature Engineering 
## Remove NULL CustomerID
## df_clean → online_retail_silver (silver layer)
df_clean = df.filter(col("Customer ID").isNotNull()) 
 

# COMMAND ----------

## Remove invalid Quantity (negative or zero)
df_clean = df_clean.filter(col("Quantity") > 0)

# COMMAND ----------

## Remove invalid Price
df_clean = df_clean.filter(col("Price")>0)

# COMMAND ----------

## Fix Date format
from pyspark.sql.functions import to_timestamp

df_clean = df_clean.withColumn(
    "InvoiceDate",
    to_timestamp(col("InvoiceDate"))
)

# COMMAND ----------

## Add Revenue column
df_clean = df_clean.withColumn(
    "Revenue",
    col("Quantity") * col("Price")
)

# COMMAND ----------

## Step 7: Extract useful columns
from pyspark.sql.functions import year, month

df_clean = df_clean.withColumn("Year", year(col("InvoiceDate")))
df_clean = df_clean.withColumn("Month", month(col("InvoiceDate")))

# COMMAND ----------

## Step 8: Check cleaned data
df_clean.show(5) 

# COMMAND ----------

## Fix column names (Spark (Delta tables) does NOT allow spaces in column names)
for col_name in df_clean.columns:
    new_col = col_name.replace(" ", "_")
    df_clean = df_clean.withColumnRenamed(col_name, new_col)

# COMMAND ----------

## Saves data as a Delta table, Stores it in Databricks, Creates new table: online_retail_silver
df_clean.write.format("delta").mode("overwrite").saveAsTable("online_retail_silver")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS online_retail_silver")

# COMMAND ----------

df_clean.write.format("delta").mode("overwrite").saveAsTable("online_retail_silver")

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM online_retail_silver")
df_silver.show(5)

# COMMAND ----------

## Verify
df_silver = spark.sql("SELECT * FROM online_retail_silver")
df_silver.show(5)

# COMMAND ----------

# Handled schema inconsistency: cleaned column names (removed spaces/special chars)
# since Delta Lake does not support invalid column naming conventions

# COMMAND ----------

# Build Gold Layer

# COMMAND ----------

## Revenue by Country (top-performing regions)
df_country = df_silver.groupBy("Country") \
    .sum("Revenue") \
    .withColumnRenamed("sum(Revenue)", "TotalRevenue")

df_country.show()

# COMMAND ----------

## Monthly Revenue Trend (growth analysis)
df_monthly = df_silver.groupBy("Year","Month") \
    .sum("Revenue") \
        .withColumnRenamed("sum(Revenue)", "MonthlyRevenue") \
            .orderBy("Year", "Month")
df_monthly.show()

# COMMAND ----------

## Top Customers (Top customers by revenue)
df_customers = df_silver.groupBy("Customer_ID") \
    .sum("Revenue") \
    .withColumnRenamed("sum(Revenue)", "CustomerRevenue") \
    .orderBy("CustomerRevenue", ascending=False)

df_customers.show(10)

# COMMAND ----------

## Top Products (Best-performing products)
df_products = df_silver.groupBy("Description") \
    .sum("Revenue") \
    .withColumnRenamed("sum(Revenue)", "ProductRevenue") \
    .orderBy("ProductRevenue", ascending=False)

df_products.show(10)

# COMMAND ----------

## Save Gold Tables
df_country.write.format("delta").mode("overwrite").saveAsTable("gold_country_revenue")

df_monthly.write.format("delta").mode("overwrite").saveAsTable("gold_monthly_revenue")

df_customers.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_customer_revenue")

df_products.write.format("delta").mode("overwrite").saveAsTable("gold_product_revenue")

# COMMAND ----------

# This above is Business Aggregation Layer / Gold Layer
# Gold Layer: Aggregated business datasets (country, monthly, customer, product revenue insights)

# COMMAND ----------

df_country.toPandas()

# COMMAND ----------

toPandas().to_csv("country.csv", index=False)

# COMMAND ----------

df_monthly.toPandas().to_csv("monthly.csv", index=False)
df_customers.toPandas().to_csv("customers.csv", index=False)
df_products.toPandas().to_csv("products.csv", index=False)

# COMMAND ----------

