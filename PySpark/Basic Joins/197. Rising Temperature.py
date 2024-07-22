##197. Rising Temperature

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import datetime
from pyspark.sql.functions import col, date_add

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define schema for Weather table
weather_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("recordDate", DateType(), True),
    StructField("temperature", IntegerType(), True)
])

# Sample data for Weather table
weather_data = [
    (1, datetime.strptime("2015-01-01", "%Y-%m-%d").date(), 10),
    (2, datetime.strptime("2015-01-02", "%Y-%m-%d").date(), 25),
    (3, datetime.strptime("2015-01-03", "%Y-%m-%d").date(), 20),
    (4, datetime.strptime("2015-01-04", "%Y-%m-%d").date(), 30)
]

# Create DataFrame
weather_df = spark.createDataFrame(weather_data, schema=weather_schema)

temp_df=weather_df.withColumn('recordDate',date_add(col('recordDate'),1))\
                  .withColumnRenamed('temperature','t_temperature')\
                  .withColumnRenamed('id','t_id')

#weather_df.show()
#temp_df.show()

weather_df.join(temp_df,weather_df.recordDate==temp_df.recordDate).filter(weather_df.temperature>temp_df.t_temperature).select(weather_df.id).show()

# Stop SparkSession
spark.stop()
