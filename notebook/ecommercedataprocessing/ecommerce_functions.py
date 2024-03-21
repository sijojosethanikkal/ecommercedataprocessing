import logging
from datetime import datetime
import s3fs
from pyspark.sql.functions import col

# Function to check if a file exists
def fileExists(filePath):
    if filePath.startswith("s3://"):
        fs = s3fs.S3FileSystem()
        try:
            fs.head(filePath)
            return True
        except FileNotFoundError:
            return False
    else:
        return False

# Chek if dataframe is empty
def dataframeEmpty(df):
  if df.isEmpty():
    return True
  else:
    return False


# Check if any column is fully null
def checkColumnsNull(df):
    for column in df.columns:
        if df.select(col(column)).filter(col(column).isNull()).count() == df.count():
            return False
    return True

# Check if the dataframe has more than one row
def checkRowCount(df):
    if df.count() > 1:
        return True
    else:
        return False

# Check if the columns exist in the dataframe
def checkColumnExists(df, columnNames):
    return all(column in df.columns for column in columnNames)

# Write dataframe to S3 in csv format.
def writeToS3(df, bucketName, fileName):
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    folder_path = f"year={year}/month={month}/day={day}"
    file_path = f"s3://{bucketName}/{folder_path}/{fileName}"

    if file_path.startswith("s3://"):
        # Write dataframe to S3 as CSV with index=False and mode="overwrite"
        df.write.mode("overwrite").option("header", "true").option("index", "false").csv(file_path)
        logging.info(f"Dataframe successfully written to {file_path}")
        return True
    else:
        logging.error("Invalid S3 file path.")
        return False