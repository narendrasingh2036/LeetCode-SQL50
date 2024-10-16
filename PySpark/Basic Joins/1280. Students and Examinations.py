# 1280. Students and Examinations

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("learning").getOrCreate()

# Define schema for Students table
students_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("student_name", StringType(), True)
])

# Create Students DataFrame
students_data = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

students_df = spark.createDataFrame(students_data, schema=students_schema)

# Define schema for Subjects table
subjects_schema = StructType([
    StructField("subject_name", StringType(), True)
])

# Create Subjects DataFrame
subjects_data = [
    ("Math",),
    ("Physics",),
    ("Programming",)
]

subjects_df = spark.createDataFrame(subjects_data, schema=subjects_schema)

# Define schema for Examinations table
examinations_schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("subject_name", StringType(), True)
])

# Create Examinations DataFrame
examinations_data = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

examinations_df = spark.createDataFrame(examinations_data, schema=examinations_schema)

# Show the DataFrames
#students_df.show()
#subjects_df.show()
#examinations_df.show()


s_df=students_df.crossJoin(subjects_df)
#s_df.show()
df=s_df.join(examinations_df,
             (s_df['student_id']==examinations_df['student_id']) & (s_df['subject_name']==examinations_df['subject_name']),
             "left").select(
                            s_df.student_id,s_df.student_name,s_df.subject_name,examinations_df.student_id.alias('attended')
                            )

#df.show()

result_df=df.groupBy(col('student_id'),col('student_name'),col('subject_name'))\
            .agg(count(col('attended'))).alias('attended_exams')
    
result_df.show()
