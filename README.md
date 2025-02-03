# Project Structure

This project consists of two main directories: **main** and **test**. Below is an outline of the files within each directory:


### Directory Details

- **main/**:
    - **Ecommerce_ETL.ipynb**: Contains the main code for extracting, transforming, and loading (ETL) Ecommerce data. This includes various operations like data cleaning, transformation, and aggregations.
  
- **test/**:
    - **Ecommerce_test.ipynb**: Contains unit tests for verifying the functionality of the ETL logic. This file ensures that all transformations are correctly applied, and that the system behaves as expected.

# Data Flow

The data flows through three main layers: **Raw (Bronze)**, **Enriched (Silver)**, and **Gold**. Below is a detailed breakdown of each layer and the corresponding tables:

## Bronze Layer (Raw)
This layer contains the raw, untransformed data.

- **raw_customer**: Raw data for customer information.
- **raw_product**: Raw data for product information.
- **raw_order**: Raw data for orders.

## Silver Layer (Enriched)
This layer contains the enriched data after applying transformations like cleaning, joining, and standardization.

- **enriched_customer**: Enriched customer data with cleaned customer names, standardized phone numbers, etc.
- **enriched_product**: Enriched product data with formatted price values
- **enriched_order**: Enriched order data with customer and product information joined, and profit calculations.

## Gold Layer (Aggregated)
This layer contains the aggregated and finalized data, ready for analysis.

- **master_order**: Aggregated data that includes profit by year, product category, sub-category, and customer.
