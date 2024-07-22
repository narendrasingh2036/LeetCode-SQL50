##1581. Customer Who Visited but Did Not Make Any Transactions

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define schema for Visits table
visits_schema = StructType([
    StructField("visit_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True)
])

# Define schema for Transactions table
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("visit_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

# Sample data for Visits table
visits_data = [
    (1, 23),
    (2, 9),
    (4, 30),
    (5, 54),
    (6, 96),
    (7, 54),
    (8, 54)
]

# Sample data for Transactions table
transactions_data = [
    (2, 5, 310),
    (3, 5, 300),
    (9, 5, 200),
    (12, 1, 910),
    (13, 2, 970)
]

# Create DataFrames
visits_df = spark.createDataFrame(visits_data, schema=visits_schema)
transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

temp_df=visits_df.join(transactions_df,visits_df.visit_id==transactions_df.visit_id,'left')\
            .filter(transactions_df['visit_id'].isNull())\
            .select('customer_id')

result_df=temp_df.groupBy('customer_id').agg(count('*')).alias('COUNT_NO_TRANS')

result_df.show()

# Stop SparkSession
spark.stop()
