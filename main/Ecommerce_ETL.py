# Databricks notebook source
# Import required libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, DecimalType
import re


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion to raw table

# COMMAND ----------

def read_csv(spark, file_path):
    # Reads a CSV file and returns a DataFrame
    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(file_path)
    return df

def read_excel(spark, file_path):
    # Reads an Excel file and returns a DataFrame
    # Requires external library to read excel files
    df = spark.read.format("com.crealytics.spark.excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(file_path)
    return df

def read_json(spark, file_path):
    # Reads a JSON file and returns a DataFrame
    df = spark.read.format("json")\
        .option("inferSchema", "true")\
        .load(file_path)
    return df

def create_table(df, table_name, path):
    # Saves data into Delta tables
    df.write.format("delta")\
        .mode("append")\
        .save(f"/mnt/{path}/{table_name}")
    
    # Register table for querying and schema evolution
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '/mnt/{path}/{table_name}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleanup and creating enriched tables

# COMMAND ----------

def format_phone_number(phone):
    # Remove non-numeric characters
    digits = re.sub(r"\D", "", phone)  
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return None

# Create UDF for above function
format_phone_udf = udf(format_phone_number, StringType())

def clean_customer_data(df):
    # Data cleanup in customer table
    # 1. Remove all non-alphabetical characters from Customer Name field
    # 2. Standardize Phone field to standard format of (XXX) XXX-XXXX 

    df_cleaned = df.withColumn("Customer Name", trim(regexp_replace(col("Customer Name"), r"[^a-zA-Z\s']", "")))\
                    .withColumn("Phone", format_phone_udf(col("phone")))
    return df_cleaned

def clean_product_data(df):
    # Data cleanup in product table
    # 1. Round up price values to 2 decimal places

    df_cleaned = df.withColumn("price", round(col("price"), 2).cast(DecimalType(10, 2)))
    return df_cleaned

def enrich_order_data(order_df, customer_df, product_df):
    # Enrich order data to create a master table
    # 1. Join customer_df to get Customer Name and Country
    # 2. Join product_df to get Category and Sub-Category
    # 3. Round up profit values to 2 decimal places

    cust_df = customer_df.select(col("Customer ID"), col("Customer Name"), col("Country")).drop_duplicates()
    prd_df = product_df.select(col("Product ID"), col("Category"), col("Sub-Category")).drop_duplicates()
    order_enrich_df = order_df.join(cust_df, cust_df["Customer ID"]==order_df["Customer ID"], how="left")\
                            .join(prd_df, prd_df["Product ID"]==order_df["Product ID"], how="left")\              
                            .withColumn("Profit", round(col("profit"), 2).cast(DecimalType(10, 2)))\
                            .select(["Order ID, Order Date", "Ship Date", "Ship Mode", "Customer Name", "Country", "Category", "Sub-Category", "Quantity", "Price", "Discount", "Profit"])
    return order_enrich_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Aggregate table

# COMMAND ----------

def aggregate_profit_data(order_df):
    # Aggregates profit by year, category, sub-category, and customer.

    # Extract year from the order_date
    order_df = order_df.withColumn("Year", year(col("Order Date")))

    # Group by year, Category, Sub-category and Customer Name
    master_data = order_df.groupBy("Year", "Category", "Sub-Category", "Customer Name") \
                        .agg(sum("Profit").alias("profit"))
    master_data = master_data.select(["Year", "Category", "Sub-Category", "Customer Name", "Profit"])\
            .drop_duplicates()
    return master_data


# COMMAND ----------

# Initialize spark session
spark = SparkSession.builder.appName("Ecommerce").getOrCreate()

# File paths (Databricks DBFS or cloud storage)
csv_path = "/mnt/data/product.csv"
excel_path = "/mnt/data/customer.xlsx"
json_path = "/mnt/data/order.json"

# Read files
product_df = read_csv(spark, csv_path)
customer_df = read_excel(spark, excel_path)
order_df = read_json(spark, json_path)

# Create raw tables (Bronze layer)
create_table(product_df, "raw_product", 'bronze')
create_table(customer_df, "raw_customer", 'bronze')
create_table(order_df, "raw_order", 'bronze')

# Data cleanup for customer and product tables (Silver layer)
customer_enriched_df = clean_customer_data(customer_df)
product_enriched_df = clean_product_data(product_df)
create_table(customer_enriched_df, "enriched_customer", 'silver')
create_table(product_enriched_df, "enriched_order", 'silver')

# Enrich order data (Silver layer)
order_enriched_df = enrich_order_data(order_df, customer_enriched_df, product_enriched_df)
create_table(order_enriched_df, "enriched_order", 'silver')

# Create master table for aggregation (Gold layer)
order_master_df = aggregate_profit_data(order_enriched_df)
create_table(order_master_df, "master_order", 'gold')


# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL queries on aggregate data

# COMMAND ----------

# Create a temporary view of the aggregated data
order_master_df.createOrReplaceTempView("order_master_view")

# a. Profit by year
profit_by_year_df = spark.sql("""
    SELECT year, SUM(profit) AS total_profit
    FROM order_master_view
    GROUP BY year
    ORDER BY year
""").show()

# b. Profit by year + category
profit_by_year_category_df = spark.sql("""
    SELECT year, Category, SUM(profit) AS total_profit
    FROM order_master_view
    GROUP BY year, Category
    ORDER BY year, Category
""").show()

# c. Profit by customer
profit_by_customer_df = spark.sql("""
    SELECT `Customer Name`, SUM(profit) AS total_profit
    FROM order_master_view
    GROUP BY `Customer Name`
    ORDER BY `Customer Name`
""").show()

# d. Profit by customer + year
profit_by_year_category_df = spark.sql("""
    SELECT `Customer Name`, year, SUM(profit) AS total_profit
    FROM order_master_view
    GROUP BY `Customer Name`, year
    ORDER BY `Customer Name`, year
""").show()

