#595. Big Countries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.types import StructType,StructField,StringType,LongType

spark=SparkSession.builder.appName('learning').getOrCreate()


schema = StructType([
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("area", LongType(), True),
    StructField("population", LongType(), True),
    StructField("gdp", LongType(), True)
])

# Data for the World table
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

result_df=df.filter((col('area')>=3000000) | (col('population')>=25000000))

result_df.show()