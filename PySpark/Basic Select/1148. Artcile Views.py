#1148. Article Views
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date

# Create a Spark session
spark = SparkSession.builder \
    .appName("Create Views Table") \
    .getOrCreate()

# Define the schema for the Views table
schema = StructType([
    StructField("article_id", IntegerType(), True),
    StructField("author_id", IntegerType(), True),
    StructField("viewer_id", IntegerType(), True),
    StructField("view_date", StringType(), True)
])

# Sample data for the Views table (with string dates)
data = [
    (1, 3, 5, '2019-08-01'),
    (1, 3, 6, '2019-08-02'),
    (2, 7, 7, '2019-08-01'),
    (2, 7, 6, '2019-08-02'),
    (4, 7, 1, '2019-07-22'),
    (3, 4, 4, '2019-07-21'),
    (3, 4, 4, '2019-07-21')
]

# Create a DataFrame from the data with correct schema
views_df = spark.createDataFrame(data, schema=schema)

# Show the content of the DataFrame
#views_df.show()

result_df=views_df.filter(col('author_id')==col('viewer_id')).select(col('author_id').alias('id')).orderBy(col('author_id'))

result_df=result_df.distinct().orderBy(col('id'))

result_df.show()