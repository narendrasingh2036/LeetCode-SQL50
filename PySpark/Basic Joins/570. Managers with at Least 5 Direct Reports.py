#577. Managers with at Least 5 Direct Reports

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("learning").getOrCreate()

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("managerId", IntegerType(), True)
])

# Data
data = [
    (101, "John", "A", None),
    (102, "Dan", "A", 101),
    (103, "James", "A", 101),
    (104, "Amy", "A", 101),
    (105, "Anne", "A", 101),
    (106, "Ron", "B", 101)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
#df.show()


manager_df = df.alias("mgr")   # rows that will play the “manager” role
emp_df     = df.alias("emp")   # rows that will play the “employee” role


df_join = (manager_df.join(emp_df,col("mgr.id") == col("emp.managerId"),how="inner").select(col("mgr.id").alias("manager_id"),col("mgr.name").alias("manager_name")))

df_group= df_join.groupBy('manager_name').agg(count('manager_id').alias('manager_count')).filter(col('manager_count')>=5).select('manager_name')

display(df_group)
