#1661. Average Time of Process per Machine

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col,avg,round

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("machine_id", IntegerType(), False),
    StructField("process_id", IntegerType(), False),
    StructField("activity_type", StringType(), False),
    StructField("timestamp", FloatType(), False)
])

# Define the data for the DataFrame
data = [
    (0, 0, "start", 0.712),
    (0, 0, "end", 1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end", 4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end", 1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end", 1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end", 4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end", 5.000)
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
#df.show()
df1 = df.alias("df1")
df2 = df.alias("df2")

activity_df = df1.join(
    df2,
    (col("df1.machine_id") == col("df2.machine_id")) &
    (col("df1.process_id")==col("df2.process_id")) &
    (col("df1.activity_type") == 'start') &
    (col("df2.activity_type") == 'end'),
    'inner'
).select(
    col("df1.machine_id"),
    col("df1.timestamp").alias('start_time'),
    col("df2.timestamp").alias('end_time')
)
#activity_df.show()

result_df = activity_df.withColumn("time_difference", col("end_time") - col("start_time")) \
                      .groupBy('machine_id') \
                      .agg(avg("time_difference").alias("processing_time")).orderBy('machine_id')

result_df=result_df.withColumn('processing_time',round('processing_time',3))

result_df.show()
