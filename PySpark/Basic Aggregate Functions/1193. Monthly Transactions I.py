from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import to_date, date_format, col,count,when,sum

spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),   
    StructField("amount", IntegerType(), True),
    StructField("trans_date", StringType(), True)
])

# Create data
data = [
    (121, "US", "approved", 1000, "2018-12-18"),
    (122, "US", "declined", 2000, "2018-12-19"),
    (123, "US", "approved", 2000, "2019-01-01"),
    (124, "DE", "approved", 2000, "2019-01-07")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# If trans_date is interpreted as string, convert to date
df = df.withColumn("trans_date", to_date("trans_date"))


df=df.withColumn("month_year", date_format("trans_date", "MM-yyyy"))

df_result=df.groupBy("month_year","country")\
          .agg(count("*").alias("trans_count"),\
               sum(when(df.state=="approved",1).otherwise(0)).alias("approved_count"),\
               sum("amount").alias("trans_total_count"),\
               sum(when(df.state=="approved",col("amount")).otherwise(0)).alias("approved_total_count"))\
          .select(col("month_year").alias("month"),"country","trans_count","approved_count","trans_total_count","approved_total_count")

display(df_result)