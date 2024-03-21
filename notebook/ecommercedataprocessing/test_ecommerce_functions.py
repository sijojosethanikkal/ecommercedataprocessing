from ecommerce_functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType,LongType, DoubleType

spark = SparkSession.builder \
                    .appName('ecommerce-tests') \
                    .getOrCreate()

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

data = [
    (1, "CA-2016-122581", "21/8/2016", "25/8/2016", "Standard Class", "JK-15370", "FUR-CH-10002961", 7, 573.174, 0.3, 63.686),
    (2, "CA-2017-117485", "23/9/2017", "29/9/2017", "Standard Class", "BD-11320", "TEC-AC-10004659", 4, 291.96, 0.5, 102.186)
]


df = spark.createDataFrame(data, order_schema)
df_null = spark.createDataFrame([(1, "CA-2016-122581", "21/8/2016", "25/8/2016", "Standard Class", "JK-15370", "FUR-CH-10002961", 7, None, 0.3, 63.686)], order_schema)

empty_df = spark.createDataFrame([], order_schema)

pandas_df = df.toPandas()
pandas_df.to_csv('output.csv', index=False)


def test_fileExists():
  assert fileExists('output.csv') is True

def test_nonFileExists():
  assert fileExists('output_01.csv') is False

def test_dataframeEmpty():
    assert dataframeEmpty(empty_df) is True

def test_dataframNoneEmpty():
    assert dataframeEmpty(df) is False

def test_checkColumnsNull():
    assert checkColumnsNull(df) is True

def test_checkEmptyColumnsNull():
    assert checkColumnsNull(df_null) is False

def test_checkRowCount():
    assert checkRowCount(df) is True

def test_checkRowCountEmpty():
    assert checkRowCount(empty_df) is False

def test_checkColumnExists():
    assert checkColumnExists(df, ["Order ID", "Ship Date"]) is True

def test_checkColumnNotExists():
    assert checkColumnExists(df, ["Order ID", "Customer Name"]) is False

def test_writeToS3_validPath():
    bucketName = "ecommecreprocessing"
    fileName = "customer_product_counts"
    result = writeToS3(df, bucketName, fileName)
    assert result is True

def test_writeToS3_invalidPath():
    bucketName = ""
    fileName = "customer_product_counts"
    result = writeToS3(df, bucketName, fileName)
    assert result is False