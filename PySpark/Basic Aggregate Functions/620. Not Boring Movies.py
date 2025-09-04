from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder.appName("CinemaTable").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("movie", StringType(), True),
    StructField("description", StringType(), True),
    StructField("rating", FloatType(), True)
])

# Create data
data = [
    (1, "War", "great 3D", 8.9),
    (2, "Science", "fiction", 8.5),
    (3, "irish", "boring", 6.2),
    (4, "Ice song", "Fantacy", 8.6),
    (5, "House card", "Interesting", 9.1)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show table
#df.show()


df=df.filter(~col("description").like("%boring%"))

df_result=df.filter(col('id')%2==1).orderBy(col('rating').desc())

df_result.show()

