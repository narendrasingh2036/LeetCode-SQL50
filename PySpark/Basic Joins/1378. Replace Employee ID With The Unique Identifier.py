from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("learning") \
    .getOrCreate()

# Define schema for Employees table
employees_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define schema for EmployeeUNI table
employeeuni_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("unique_id", IntegerType(), True)
])

# Sample data for Employees table
employee_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# Sample data for employeeuni table
employeeuni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# Create DataFrames
employees_df = spark.createDataFrame(employee_data, schema=employees_schema)
employeeuni_df = spark.createDataFrame(employeeuni_data, schema=employeeuni_schema)

#employees_df.show()

#employeeuni_df.show()

result=employees_df.join(employeeuni_df,employees_df.id==employeeuni_df.id,'left').select(employeeuni_df.unique_id,employees_df.name)

result.show()


# Stop SparkSession
spark.stop()
