from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,count,round

# Spark session
spark = SparkSession.builder.appName("CreateUsersRegister").getOrCreate()

# ----- Users -----
users_data = [
    (6, "Alice"),
    (2, "Bob"),
    (7, "Alex"),
]
users_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("user_name", StringType(), nullable=False),
])
users_df = spark.createDataFrame(users_data, schema=users_schema)

# ----- Register -----
register_data = [
    (215, 6),
    (209, 2),
    (208, 2),
    (210, 6),
    (208, 6),
    (209, 7),
    (209, 6),
    (215, 7),
    (208, 7),
    (210, 2),
    (207, 2),
    (210, 7),
]
register_schema = StructType([
    StructField("contest_id", IntegerType(), nullable=False),
    StructField("user_id", IntegerType(), nullable=False),
])
register_df = spark.createDataFrame(register_data, schema=register_schema)

#users_df.show()
#register_df.show()

df_result=register_df.groupBy(col('contest_id')).agg(count(col('user_id')).alias('registered_user_count'))

df_result=df_result.withColumn('percentage',(col('registered_user_count')/users_df.count())*100)

df_result=df_result.withColumn('percentage',round(col('percentage'),2))

df_result=df_result.orderBy([col('percentage').desc(),col('contest_id').asc()])

df_result.show()
