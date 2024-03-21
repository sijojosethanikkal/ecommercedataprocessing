
# E-commerce Sales Data Processing with Databricks

## Purpose

This notebook gives you the data processing for E-commerce platform


## Prerequisites

The required files for this project are stored in an Amazon S3 bucket.

## Program Exit Conditions

The program will exit in the following cases:

1. **File Not Present**: The program will exit if the required file is not present in the S3 bucket.

2. **Empty File Data**: The program will exit if the entire file data is empty.


## Flow Diagram
 [Flow Diagram](flowdiagram/ecommercedataprocessing_flow_chart.png)
 
## Running the Program

Ensure that the required files are present and not empty in the S3 bucket before running the program.

Check mode before running program.
1. Mode - Test, to validate the results after each report generation.
2. Mode - Dev, Prod. To run the program and write the result back to s3.


## Approach.
1. The generic functions like file check, data check and write the result to s3 are written in `ecommerce_functions.py`
2. Read all the files.
   1. Check reading issue in file.
   2. Check for empty data frame.
   3. check for any of the all column is empty
   4. Modify the column data type. If the column coming in Stiring.
3. Create a master data frame. For all the subsequent query analysis.
4. Create order information data frame.
5. Create aggregation data frame.
6. Create a temp view to explore data in sql.
   1. Create profit by year
   2. Create Profit by Year + Product Category
   3. Create Profit by Customer
   4. Create Profit by Customer + Year
7. Write files to s3 bucket, if the mode is not test. 




## Assumptions
1. In order to read the execl file. The `com.crealytics.spark.excel` should be available in databricks

## Enhanced Function.
 Created a write to s3 file as csv. This function is called under each data processing. The files are stored in the given s3 bucket, seperated by current year,month and day. Error is logged for any failures.
### Optimisation/Future Enhancement
Currently the csv file is written under a folder by spark job, which creates multiple partition. To better readbaility, we can convert the spark df to pandas and write as csv. After analysing the memory consumption and file size.
## Validations/ Unit test
 
1. The generic functions are validated by the test file.

Please use following to run the test file.
```
%pip install pytest
```
Install pyt test
```
import pytest
import os
import sys

repo_name = "<my-repo-name>"

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace/{repo_root}/{repo_name}")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
```

2. The validations under the notebook can be run by making the mode to `Test`
 ## Schedule Job.
 To run test preiodically, we can schedule job with mode as test.
 
 Note: While running in test mode, the files are not written to s3. 

The conf folder is to store the schedule job json.
