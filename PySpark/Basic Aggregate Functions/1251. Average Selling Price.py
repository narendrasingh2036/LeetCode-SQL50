from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from datetime import date
from pyspark.sql.functions import col, sum as spark_sum,round, coalesce,lit

spark = SparkSession.builder.appName("CreateTables").getOrCreate()

# ----------------------------
# Prices Table
# ----------------------------

prices_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("price", IntegerType(), True)
])

prices_data = [
    (1, date(2019, 2, 17), date(2019, 2, 28), 5),
    (1, date(2019, 3, 1), date(2019, 3, 22), 20),
    (2, date(2019, 2, 1), date(2019, 2, 20), 15),
    (2, date(2019, 2, 21), date(2019, 3, 31), 30)
]

prices_df = spark.createDataFrame(prices_data, schema=prices_schema)
prices_df.createOrReplaceTempView("Prices")

# ----------------------------
# UnitsSold Table
# ----------------------------

units_sold_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("purchase_date", DateType(), True),
    StructField("units", IntegerType(), True)
])

units_sold_data = [
    (1, date(2019, 2, 25), 100),
    (1, date(2019, 3, 1), 15),
    (2, date(2019, 2, 10), 200),
    (2, date(2019, 3, 22), 30)
]

units_sold_df = spark.createDataFrame(units_sold_data, schema=units_sold_schema)
units_sold_df.createOrReplaceTempView("UnitsSold")

# Show tables
#prices_df.show()
#units_sold_df.show()

df=prices_df.alias("p").join(units_sold_df.alias("u"),\
                             (col("u.product_id") == col("p.product_id")) &\
                             (col("u.purchase_date") >= col("p.start_date")) &\
                             (col("u.purchase_date") <= col("p.end_date")),how="left" )\
            .select(col("p.product_id"),col("p.price"),col("u.units"))

df.show()

df=df.groupBy("product_id")\
               .agg(spark_sum(col("price")*col("units")).alias("price_units")\
               ,spark_sum(col("units")).alias("units_sum"))

df=df.withColumn("average_price",coalesce(round(col("price_units")/col("units_sum"),2),lit(0)))

df.show()



