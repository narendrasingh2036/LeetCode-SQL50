from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

spark=SparkSession.builder.appName('learning').getOrCreate()

data = [['0', 'Y', 'N'], ['1', 'Y', 'Y'], ['2', 'N', 'Y'], ['3', 'Y', 'Y'], ['4', 'N', 'N']]
products = pd.DataFrame(data, columns=['product_id', 'low_fats', 'recyclable']).astype({'product_id':'int64', 'low_fats':'category', 'recyclable':'category'})

spark_df = spark.createDataFrame(products)

result_df=spark_df.filter((col('low_fats')=='Y') & (col('recyclable')=='Y'))

result_df.select(col('product_id')).show()