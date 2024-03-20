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
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
import sys
from pyspark.sql.functions import col, to_date,regexp_extract
from pyspark.sql import functions as F
from pyspark.sql.functions import round
from pyspark.sql.functions import year

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


# COMMAND ----------

# Retriive from paramters
order_file = dbutils.widgets.get("order_file")
customer_file = dbutils.widgets.get("customer_file")
product_file = dbutils.widgets.get("product_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load order data frame

# COMMAND ----------

try:
  # Read JSON data with explicit schema.
    order_df = spark.read.option("multiLine", "true").schema(order_schema).json(order_file)
except Exception as e:
  print("Error occurred while reading JSON order data. Please check!. Error: ", e)
  sys.exit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for any error in df

# COMMAND ----------

# Chek the data frame is totaly empty
if order_df.isEmpty():
  print("order data frame is empty")
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
for column in order_df.columns:
    if order_df.select(col(column)).filter(col(column).isNull()).count() == order_df.count():
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
    customer_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").schema(custom_schema).load(customer_file)
except Exception as e:
  print("Error occurred while reading execl customer data. Please check!. Error: ", e)
  sys.exit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for amy error in df

# COMMAND ----------

# Chek the data frame is totaly empty
if customer_df.isEmpty():
  print("customer data frame is empty")
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
for column in customer_df.columns:
    if customer_df.select(col(column)).filter(col(column).isNull()).count() == order_df.count():
        logging.warning(f"Column '{column}' is fully null.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load product data frame

# COMMAND ----------

try:
  # Read csv data with explicit schema.
    product_df = spark.read.csv(product_csv, header=True, schema=product_schema)
except Exception as e:
  print("Error occurred while reading product csv data. Please check!. Error: ", e)
  sys.exit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for any error in df

# COMMAND ----------

# Chek the data frame is totaly empty
if product_df.isEmpty():
  print("product data frame is empty")
  sys.exit(1)

# COMMAND ----------

# Check if any column is fully null
for column in product_df.columns:
    if product_df.select(col(column)).filter(col(column).isNull()).count() == product_df.count():
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

# MAGIC %md
# MAGIC ## Create an enriched table which has
# MAGIC 1. order information 
# MAGIC     1. Profit rounded to 2 decimal places
# MAGIC 2. Customer name and country
# MAGIC 3. Product category and sub category
# MAGIC

# COMMAND ----------

order_info = master_df.select('Order ID','Profit','Customer Name','Country','Category','Sub-Category')
order_info.show()
order_info = order_info.select(round(order_info["Profit"], 2).alias("Profit"))
order_info.show()

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

agg_df.show()


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

# Show the resulting DataFrame
profit_by_year_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year + Product Category

# COMMAND ----------

profit_by_year_category_df = spark.sql("SELECT year, Category, ROUND(SUM(Profit),2) AS TotalProfit FROM master_table GROUP BY year, Category ORDER BY year, Category")

profit_by_year_category_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer

# COMMAND ----------

profit_by_customer_df = spark.sql("SELECT `Customer Name`, Round(SUM(Profit),2) AS TotalProfit FROM master_table GROUP BY `Customer Name` order by TotalProfit DESC")
profit_by_customer_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Customer + Year

# COMMAND ----------

profit_by_customer_year_df = spark.sql("SELECT `Customer Name`, year, Round(SUM(Profit), 2) AS TotalProfit FROM master_table GROUP BY `Customer Name`, year ORDER BY year, TotalProfit DESC")

profit_by_customer_year_df.show()
