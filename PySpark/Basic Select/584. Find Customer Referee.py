from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import pandas as pd

spark=SparkSession.builder.appName('learning').getOrCreate()

data = [[1, 'Will', None], [2, 'Jane', None], [3, 'Alex', 2], [4, 'Bill', None], [5, 'Zack', 1], [6, 'Mark', 2]]

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])
spark_df=spark.createDataFrame(data,schema)

result=spark_df.filter((col('referee_id')!=2) | (col('referee_id').isNull())).select('name')

result.show()