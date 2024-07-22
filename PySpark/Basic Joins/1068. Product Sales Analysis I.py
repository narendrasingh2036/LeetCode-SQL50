from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define schema for Sales table
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

# Define schema for Product table
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True)
])

# Sample data for Sales table
sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

# Sample data for Product table
product_data = [
    (100, "Nokia"),
    (200, "Apple"),
    (300, "Samsung")
]

# Create DataFrames
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)
product_df = spark.createDataFrame(product_data, schema=product_schema)


result_df=sales_df.join(product_df,sales_df.product_id==product_df.product_id,'inner').select(product_df.product_name,sales_df.year,sales_df.price)

result_df.show()

# Stop SparkSession
spark.stop()
