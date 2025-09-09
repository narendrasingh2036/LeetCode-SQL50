from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,avg,round


spark = SparkSession.builder.getOrCreate()

# ---------- Schemas ----------
project_schema = StructType([
    StructField("project_id", IntegerType(), nullable=False),
    StructField("employee_id", IntegerType(), nullable=False),
])

employee_schema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("experience_years", IntegerType(), nullable=False),
])

# ---------- Data ----------
project_data = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4),
]

employee_data = [
    (1, "Khaled", 3),
    (2, "Ali", 2),
    (3, "John", 1),
    (4, "Doe", 2),
]

# ---------- DataFrames ----------
df_project = spark.createDataFrame(project_data, schema=project_schema)
df_employee = spark.createDataFrame(employee_data, schema=employee_schema)

#df_project.show()
#df_employee.show()

df=df_project.join(df_employee,df_project.employee_id==df_employee.employee_id,how='INNER')\
    .select(col('project_id'),col('experience_years'))

df_result=df.groupBy(col('project_id')).agg(round(avg(col('experience_years')),2).alias('average_years'))

df_result.show()


