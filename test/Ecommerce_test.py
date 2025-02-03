# Databricks notebook source
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# COMMAND ----------

# Import all functions from Main Notebook
%run "/Workspace/Ecommerce/Ecommerce_ETL"

# COMMAND ----------

# Create spark session for testing
@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").getOrCreate()

# COMMAND ----------

# Test data ingestion functions
def test_read_csv(spark):
    df = read_csv(spark, "/mnt/test/sample_product.csv")
    assert df is not None
    assert df.count() > 0

def test_read_excel(spark):
    df = read_excel(spark, "/mnt/test/sample_customer.xlsx")
    assert df is not None
    assert df.count() > 0

def test_read_json(spark):
    df = read_json(spark, "/mnt/test/sample_order.json")
    assert df is not None
    assert df.count() > 0


# COMMAND ----------

# Test data transformation functions
def test_clean_customer_data(spark):
    test_data = [
        ("John123 Doe!@", "123-456-7890"),
        ("Mary_45 Jane$", "(987)654-3210"),
        ("$#@Mike O'Conner ", "9876543210"),
    ]
    df = spark.createDataFrame(test_data, ["customer_name", "phone_number"])
    transformed_df = clean_customer_data(df)

    expected_data = [
        ("John Doe", "(123) 456-7890"),
        ("Mary Jane", "(987) 654-3210"),
        ("Mike O'Conner", "(987) 654-3210"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["customer_name", "phone_number"])

    assert transformed_df.collect() == expected_df.collect()


def test_clean_product_data(spark):
    schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("price", DecimalType(10, 4), True)  # Original precision
    ])
    
    test_data = [("Product A", 10.4567), ("Product B", 20.7899), ("Product C", 30.1)]
    df = spark.createDataFrame(test_data, schema=schema)
    
    transformed_df = clean_product_data(df)

    expected_data = [("Product A", 10.46), ("Product B", 20.79), ("Product C", 30.10)]
    expected_schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("price", DecimalType(10, 2), True)  # Expected precision
    ])
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert transformed_df.collect() == expected_df.collect()

def test_enrich_order_data(spark):
    # Sample input data for order_df
    order_data = [
        (1, "2025-01-01", "2025-01-02", "Standard", 101, 201, 10, 100, 0.1, 900),
        (2, "2025-01-02", "2025-01-03", "Express", 102, 202, 5, 200, 0.05, 950)
    ]
    order_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"]
    order_df = spark.createDataFrame(order_data, order_columns)

    # Sample input data for customer_df
    customer_data = [
        (101, "John Doe", "USA"),
        (102, "Jane Smith", "UK")
    ]
    customer_columns = ["Customer ID", "Customer Name", "Country"]
    customer_df = spark.createDataFrame(customer_data, customer_columns)

    # Sample input data for product_df
    product_data = [
        (201, "Electronics", "Mobile"),
        (202, "Furniture", "Table")
    ]
    product_columns = ["Product ID", "Category", "Sub-Category"]
    product_df = spark.createDataFrame(product_data, product_columns)

    # Call the function to enrich the order data
    enriched_df = enrich_order_data(order_df, customer_df, product_df)

    # Expected output data
    expected_data = [
        (1, "2025-01-01", "2025-01-02", "Standard", "John Doe", "USA", "Electronics", "Mobile", 10, 100, 0.1, Decimal('900.00')),
        (2, "2025-01-02", "2025-01-03", "Express", "Jane Smith", "UK", "Furniture", "Table", 5, 200, 0.05, Decimal('950.00'))
    ]
    expected_columns = ["Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer Name", "Country", "Category", "Sub-Category", "Quantity", "Price", "Discount", "Profit"]
    
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Compare the actual result with the expected result using exceptAll
    diff_df = enriched_df.exceptAll(expected_df)

    # Assert that there are no differences between the two DataFrames
    assert diff_df.count() == 0



# COMMAND ----------

# Test data aggregation function

def test_aggregate_profit_data(spark):
    # Sample input data for order_df
    order_data = [
        ("2025-01-01", "Electronics", "Mobile", "John Doe", 100),
        ("2025-02-01", "Electronics", "Mobile", "John Doe", 200),
        ("2025-03-01", "Furniture", "Chair", "Jane Smith", 150),
        ("2025-03-01", "Furniture", "Chair", "John Doe", 100),
        ("2026-01-01", "Electronics", "Laptop", "John Doe", 300)
    ]
    order_columns = ["Order Date", "Category", "Sub-Category", "Customer Name", "Profit"]
    order_df = spark.createDataFrame(order_data, order_columns)

    # Call the function to aggregate profit data
    aggregated_df = aggregate_profit_data(order_df)

    # Expected output data (after aggregation)
    expected_data = [
        (2025, "Electronics", "Mobile", "John Doe", Decimal('300.00')),
        (2025, "Furniture", "Chair", "Jane Smith", Decimal('150.00')),
        (2025, "Furniture", "Chair", "John Doe", Decimal('100.00')),
        (2026, "Electronics", "Laptop", "John Doe", Decimal('300.00'))
    ]
    expected_columns = ["Year", "Category", "Sub-Category", "Customer Name", "Profit"]

    # Create the expected DataFrame
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Compare the actual result with the expected result using exceptAll
    diff_df = aggregated_df.exceptAll(expected_df)

    # Assert that there are no differences between the two DataFrames
    assert diff_df.count() == 0



