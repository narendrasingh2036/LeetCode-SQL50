#577. Employee Bonus

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define schema for Employee table
employee_schema = StructType([
    StructField("empId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("supervisor", IntegerType(), True),
    StructField("salary", IntegerType(), False)
])

# Define schema for Bonus table
bonus_schema = StructType([
    StructField("empId", IntegerType(), False),
    StructField("bonus", IntegerType(), False)
])

# Data for Employee table
employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

# Data for Bonus table
bonus_data = [
    (2, 500),
    (4, 2000)
]

# Create DataFrames
employee_df = spark.createDataFrame(employee_data, schema=employee_schema)
bonus_df = spark.createDataFrame(bonus_data, schema=bonus_schema)

# Join Employee and Bonus tables on empId (left join to include all employees)
result_df = employee_df.join(bonus_df,employee_df.empId==bonus_df.empId, "left")\
                            .filter((col('bonus')<1000) | col('bonus').isNull())\
                            .select(col('name'),col('bonus'))

# Show the combined DataFrame
result_df.show()
