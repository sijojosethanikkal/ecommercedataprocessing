# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Sales Data Processing with Databricks
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook gives you the data processing for E-commerce platform
# MAGIC
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC The required files for this project are stored in an Amazon S3 bucket.
# MAGIC
# MAGIC ## Program Exit Conditions
# MAGIC
# MAGIC The program will exit in the following cases:
# MAGIC
# MAGIC 1. **File Not Present**: The program will exit if the required file is not present in the S3 bucket.
# MAGIC
# MAGIC 2. **Empty File Data**: The program will exit if the entire file data is empty.
# MAGIC
# MAGIC ## Running the Program
# MAGIC
# MAGIC Ensure that the required files are present and not empty in the S3 bucket before running the program.
# MAGIC ## Approach.
# MAGIC 1. Read all the files.
# MAGIC 2. Since the approach is to analysis on top of the files. I had created a master dataframe and create valuable insights from the data frame.
# MAGIC
# MAGIC ## Assumptions
# MAGIC 1. In order to read the execl file. The `com.crealytics.spark.excel` should be available in databricks

# COMMAND ----------

# Libraries to load
from ecommerce_functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, regexp_extract, round, year
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
import logging
import sys

# COMMAND ----------

# Order Schema
order_schema = StructType([
    StructField("Row ID", LongType()),
    StructField("Order ID", StringType()),
    StructField("Order Date", StringType()),
    StructField("Ship Date", StringType()),
    StructField("Ship Mode", StringType()),
    StructField("Customer ID", StringType()),
    StructField("Product ID", StringType()),
    StructField("Quantity", LongType()),
    StructField("Price", StringType()),
    StructField("Discount", DoubleType()),
    StructField("Profit", DoubleType())
])
# customer Schema
custom_schema = StructType([
    StructField("Customer ID", StringType()),
    StructField("Customer Name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("address", StringType()),
    StructField("Segment", StringType()),
    StructField("Country", IntegerType()),  
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("Postal Code", StringType()),
    StructField("Region", StringType())
])
#product Schema
product_schema = StructType([
    StructField("Product ID", StringType()),
    StructField("Category", StringType()),
    StructField("Sub-Category", StringType()),
    StructField("Product Name", StringType()),
    StructField("State", StringType()),
    StructField("Price per product", DoubleType())
])

# COMMAND ----------

# Read s3 files from input paramters
dbutils.widgets.text("order_file","s3://ecomerce/Order.json")
dbutils.widgets.text("customer_file","s3://ecomerce/customer.xlsx")
dbutils.widgets.text("product_file","s3://ecomerce/product.csv")
dbutils.widgets.text("outputbucket","ecommerce/output")
dbutils.widgets.dropdown("Mode","Test",["Test","Dev","Prod"])


# COMMAND ----------

# Retriive from paramters
order_file = dbutils.widgets.get("order_file")
customer_file = dbutils.widgets.get("customer_file")
product_file = dbutils.widgets.get("product_file")
outputbucket = dbutils.widgets.get("outputbucket")
mode = dbutils.widgets.get("Mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load order data frame

# COMMAND ----------

try:
  # Read JSON data with explicit schema.
    if fileExists(order_file):
      order_df = spark.read.option("multiLine", "true").schema(order_schema).json(order_file)
    if dataframeEmpty(order_df):
      print("Dataframe is empty")
except Exception as e:
  print("Error occurred while reading JSON order data. Please check!. Error: ", e)
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
if checkColumnsNull(order_df):
    print('Data frame has no empty columns')
else:
    logging.warning(f"Column '{column}' is fully null.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conversion of columns from string to date

# COMMAND ----------

# Check for any misisng rows in data frames.
try:
    filtered_df = order_df.filter(col("Order Date").isNull())
except Exception as e:
    # Log the error and the number of rows
    logging.warning(f"Order date column has missing data: {e}. Number of rows: {filtered_df.count()}")

try:
    filtered_df = order_df.filter(col("Ship Date").isNull())
except Exception as e:
    # Log the error and the number of rows
    logging.warning(f"Ship Date column has missing data: {e}. Number of rows: {filtered_df.count()}")

# COMMAND ----------

try:
    # Convert date strings to Timestamps
    order_df = order_df.withColumn("Order Date", to_date(col("Order Date"), "dd/MM/yyyy"))
    order_df = order_df.withColumn("Ship Date", to_date(col("Ship Date"), "dd/MM/yyyy"))
except Exception as e:
    logging.warning(f"Date conversion failed: {e}")

# COMMAND ----------

try:
    # Convert Price column from strings to double
    order_df = order_df.withColumn("Price", col("Price").cast("double"))
except Exception as e:
    logging.warning(f"Price column conversion failed: {e}")

# COMMAND ----------

try:
    filtered_df = order_df.filter(col("Price").isNull())
except Exception as e:
    # Log the error and the number of rows
    logging.warning(f"Price column has missing data: {e}. Number of rows: {filtered_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load customer data frame

# COMMAND ----------

try:
  # Read xslx data with explicit schema.
    if fileExists(customer_file):
      customer_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").schema(custom_schema).load(customer_file)
    if dataframeEmpty(customer_df):
      print("Dataframe is empty")
except Exception as e:
  print("Error occurred while reading execl customer data. Please check!. Error:", e)
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
if checkColumnsNull(customer_df):
    print('Data frame has no empty columns')
else:
    logging.warning(f"Column '{column}' is fully null.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load product data frame

# COMMAND ----------

try:
  # Read csv data with explicit schema.
    if fileExists(product_csv):
      product_df = spark.read.csv(product_csv, header=True, schema=product_schema)
    if dataframeEmpty(product_df):
      print("Dataframe is empty")
except Exception as e:
  print("Error occurred while reading product csv data. Please check!. Error: ", e)
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
if checkColumnsNull(product_df):
    print('Data frame has no empty columns')
else:
    logging.warning(f"Column '{column}' is fully null.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a master data frame

# COMMAND ----------

master_df = order_df.join(customer_df, on="Customer ID", how="inner").join(product_df, on="Product ID", how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an enriched table for customers and products 

# COMMAND ----------

# Group by "Customer Name" and "Product Name" and count occurrences
customer_product_counts = master_df.groupBy("Customer Name", "Product Name").count()

# Add a new column for the count
customer_product_counts = customer_product_counts.withColumn("Count", F.col("count"))
customer_product_counts.show()

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(customer_product_counts) == True
    assert checkColumnExists(customer_product_counts,["Customer Name", "Product Name","Count"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(customer_product_counts, outputbucket, 'customer_product_counts')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an enriched table which has
# MAGIC 1. order information 
# MAGIC     1. Profit rounded to 2 decimal places
# MAGIC 2. Customer name and country
# MAGIC 3. Product category and sub category
# MAGIC

# COMMAND ----------

order_info = master_df.select('Order ID','Profit','Customer Name','Country','Category','Sub-Category')
order_info = order_info.select(round(order_info["Profit"], 2).alias("Profit"))

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(order_info) == True
    assert checkColumnExists(order_info, ["Order ID", "Profit","Customer Name","Country","Category","Sub-Category"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(order_info, outputbucket, 'order_info')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create an aggregate table that shows profit by 
# MAGIC 1. Year
# MAGIC 2. Product Category
# MAGIC 3. Product Sub Category
# MAGIC 4. Customer
# MAGIC

# COMMAND ----------

master_df = master_df.withColumn("year", year("Order Date"))
agg_df = master_df.withColumn("year", year("Order Date"))
agg_df = agg_df.groupBy("year","Category", "Sub-Category", "Customer Name").sum("Profit")

agg_df = agg_df.withColumnRenamed("sum(Profit)", "Profit")
agg_df = agg_df.withColumn("Profit", round(col("Profit"), 2))
agg_df = agg_df.orderBy("year")

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(agg_df) == True
    assert checkColumnExists(agg_df, ["year", "Category","Sub-Category","Customer Name","Profit"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(agg_df, outputbucket, 'agg_table_year')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using SQL output the following aggregates
# MAGIC 1. Profit by Year
# MAGIC 2. Profit by Year + Product Category
# MAGIC 3. Profit by Customer
# MAGIC 4. Profit by Customer + Year
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by year

# COMMAND ----------

# Register the master_df as a temporary table
master_df.createOrReplaceTempView("master_table")

# Query the master_table to get Profit by Year
profit_by_year_df = spark.sql("SELECT year, ROUND(SUM(Profit),2) AS TotalProfit FROM master_table GROUP BY year ORDER BY year")

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(profit_by_year_df) == True
    assert checkColumnExists(profit_by_year_df, ["year", "TotalProfit"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(profit_by_year_df, outputbucket, 'profit_by_year')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year + Product Category

# COMMAND ----------

profit_by_year_category_df = spark.sql("SELECT year, Category, ROUND(SUM(Profit),2) AS TotalProfit FROM master_table GROUP BY year, Category ORDER BY year, Category")

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(profit_by_year_category_df) == True
    assert checkColumnExists(profit_by_year_category_df, ["year", "Category","TotalProfit"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(profit_by_year_category_df, outputbucket, 'profit_by_year_category')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer

# COMMAND ----------

profit_by_customer_df = spark.sql("SELECT `Customer Name`, Round(SUM(Profit),2) AS TotalProfit FROM master_table GROUP BY `Customer Name` order by TotalProfit DESC")

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(profit_by_customer_df) == True
    assert checkColumnExists(profit_by_customer_df, ["Customer Name","TotalProfit"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(profit_by_customer_df, outputbucket, 'profit_by_customer')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer + Year

# COMMAND ----------

profit_by_customer_year_df = spark.sql("SELECT `Customer Name`, year, Round(SUM(Profit), 2) AS TotalProfit FROM master_table GROUP BY `Customer Name`, year ORDER BY year, TotalProfit DESC")

# COMMAND ----------

# Test the row count and columns in the data frame.
if mode == 'Test':
    assert checkRowCount(profit_by_customer_year_df) == True
    assert checkColumnExists(profit_by_customer_year_df, ["Customer Name","year","TotalProfit"]) == True

# COMMAND ----------

# Write the output to csv. if the mode is not Test
if mode != 'Test':
    writeToS3(profit_by_customer_year_df, outputbucket, 'profit_by_customer_year')
