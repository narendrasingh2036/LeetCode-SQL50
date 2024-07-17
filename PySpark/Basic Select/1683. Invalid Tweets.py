# 1683. Invalid Tweets

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import length

# Create a Spark session
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define the schema for the Tweets table
schema = StructType([
    StructField("tweet_id", IntegerType(), False),
    StructField("content", StringType(), True)
])

# Sample data for the Tweets table
data = [
    (1, "Vote for Biden"),
    (2, "Let us make America great again!")
]

# Create a DataFrame from the data and schema
tweets_df = spark.createDataFrame(data, schema=schema)

# Show the content of the DataFrame
tweets_df.show()


result_df=tweets_df.withColumn('content_length',length(col('content'))).filter(col('content_length')>15).select(col('tweet_id')).orderBy('tweet_id')

result_df.show()

