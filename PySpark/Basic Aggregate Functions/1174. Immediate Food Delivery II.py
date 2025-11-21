from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType,StringType
from pyspark.sql.functions import to_date,row_number,col,when,sum,count ,round
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_pref_delivery_date", StringType(), True)
])


data = [
    (1, 1, "2019-08-01", "2019-08-02"),
    (2, 2, "2019-08-02", "2019-08-02"),
    (3, 1, "2019-08-11", "2019-08-12"),
    (4, 3, "2019-08-24", "2019-08-24"),
    (5, 3, "2019-08-21", "2019-08-22"),
    (6, 2, "2019-08-11", "2019-08-13"),
    (7, 4, "2019-08-09", "2019-08-09")
]


df = spark.createDataFrame(data, schema=schema)

df = (
    df.withColumn("order_date", to_date("order_date"))
      .withColumn("customer_pref_delivery_date", to_date("customer_pref_delivery_date"))
)

w=Window.partitionBy("customer_id").orderBy(col("order_date"))

df=df.withColumn("row_num",row_number().over(w)).filter(col("row_num")==1)\
      .withColumn("order_type",when(col("order_date")==col("customer_pref_delivery_date"),1).otherwise(0))

df_result=df.select(round(sum(col("order_type"))/count(col("order_type"))*100,2).alias("immediate_percentage"))

display(df_result)

