
# E-commerce Sales Data Processing with Databricks

## Purpose

This notebook gives you the data processing for E-commerce platform


## Prerequisites

The required files for this project are stored in an Amazon S3 bucket.

## Program Exit Conditions

The program will exit in the following cases:

1. **File Not Present**: The program will exit if the required file is not present in the S3 bucket.

2. **Empty File Data**: The program will exit if the entire file data is empty.


## Flow Diagaram
 [Flow Diagram](ecommercedataprocessing_flow_chart.png)
 
## Running the Program

Ensure that the required files are present and not empty in the S3 bucket before running the program.


## Approach.
1. Read all the files.
   1. Check reading issue in file.
   2. Check for empty data frame.
   3. check for any of the all column is empty
   4. Modify the column data type. If the column coming in Stiring.
2. Create a master data frame. For all the subsequent query analysis.
3. Create order information data frame.
4. Create aggregation data frame.
5. Create a temp view to explore data in sql.
   1. Create profit by year
   2. Create Profit by Year + Product Category
   3. Create Profit by Customer
   4. Create Profit by Customer + Year




## Assumptions
1. In order to read the execl file. The `com.crealytics.spark.excel` should be available in databricks
