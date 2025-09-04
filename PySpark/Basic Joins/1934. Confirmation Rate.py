from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

spark = SparkSession.builder.appName("SignupConfirmationTables").getOrCreate()

# ───────────────── Signups ─────────────────
signups_schema = StructType([
    StructField("user_id",   IntegerType(), False),
    StructField("time_stamp", StringType(),  False)      # load as string
])

signups_data = [
    (3, "2020-03-21 10:16:13"),
    (7, "2020-01-04 13:57:59"),
    (2, "2020-07-29 23:09:44"),
    (6, "2020-12-09 10:39:37")
]

signups_df = (spark
              .createDataFrame(signups_data, schema=signups_schema)
              .withColumn("time_stamp", F.to_timestamp("time_stamp")))   # cast to timestamp
signups_df.createOrReplaceTempView("signups")


# ──────────────── Confirmations ────────────────
confirmations_schema = StructType([
    StructField("user_id",   IntegerType(), False),
    StructField("time_stamp", StringType(),  False),     # load as string
    StructField("action",    StringType(),  False)
])

confirmations_data = [
    (3, "2021-01-06 03:30:46", "timeout"),
    (3, "2021-07-14 14:00:00", "timeout"),
    (7, "2021-06-12 11:57:29", "confirmed"),
    (7, "2021-06-13 12:58:28", "confirmed"),
    (7, "2021-06-14 13:59:27", "confirmed"),
    (2, "2021-01-22 00:00:00", "confirmed"),
    (2, "2021-02-28 23:59:59", "timeout")
]

confirmations_df = (spark
                    .createDataFrame(confirmations_data, schema=confirmations_schema)
                    .withColumn("time_stamp", F.to_timestamp("time_stamp")))  # cast to timestamp
confirmations_df.createOrReplaceTempView("confirmations")

df_join=signups_df.join(confirmations_df,signups_df.user_id==confirmations_df.user_id,how='left').select(signups_df.user_id,confirmations_df.action)

df_join=df_join.withColumn('confirm_action',when(col('action')=='confirmed',1).otherwise(0))

df_join=df_join.groupBy('user_id').agg(round(avg('confirm_action'),2).alias('confirmation_rate'))

df_join.show()


