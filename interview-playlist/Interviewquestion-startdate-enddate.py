# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Sample Data
data = [("01-06-2020", "Booked"),
        ("02-06-2020", "Booked"),
        ("03-06-2020", "Booked"),
        ("04-06-2020", "Available"),
        ("05-06-2020", "Available"),
        ("06-06-2020", "Available"),
        ("07-06-2020", "Booked")]

schema = StructType([
    StructField("show_date", StringType(), True),
    StructField("show_status", StringType(), True)
])

spark = SparkSession.builder.appName("Solution").getOrCreate()
df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# Convert show_date to date type
df = df.withColumn("show_date", F.to_date(col("show_date"), "dd-MM-yyyy"))
df.printSchema

# COMMAND ----------

# Create show_change column to identify event change in show_name
df = df.withColumn("show_change", F.lag(col("show_status")).over(Window.orderBy(col("show_date"))))
df.display()

# COMMAND ----------

df = df.withColumn("show_change", F.when(col("show_status") !=  F.lag(col("show_status")).over(Window.orderBy(col("show_date"))),1).otherwise(0)  )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate Running Sum to create Windows

# COMMAND ----------

#Create a column show_group based on cumulative sum of show_change
df2 = df.withColumn("show_group", F.sum("show_change").over(Window.orderBy("show_date")))
df2.display()

# COMMAND ----------

result_df = df2.groupBy("show_group", "show_status").agg(F.min("show_date").alias("start_date"), 
                                             F.max("show_date").alias("end_date")) \
                                               .orderBy("start_date")
result_df.display()

# COMMAND ----------

result_df = result_df.drop("show_group")
result_df.display()

# COMMAND ----------

df = df.withColumn("show_date", F.to_date("show_date", "dd-MM-yyyy"))

# Define window specification
window_spec = Window.partitionBy("show_status").orderBy("show_date")

# Assign row number within each show_status
df = df.withColumn("row_num", F.row_number().over(window_spec))
df.display()


# COMMAND ----------

# Calculate unique block identifier for each consecutive show_status
df = df.withColumn("block_id", F.expr("date_sub(show_date, row_num)"))
df.display()


# COMMAND ----------

# Group by block_id and show_status to find start and end dates
result_df = df.groupBy("show_status", "block_id") \
              .agg(F.min("show_date").alias("start_date"),
                   F.max("show_date").alias("end_date")) \
              .orderBy("start_date")

# Drop the helper column 'block_id'
result_df = result_df.drop("block_id")
result_df.show()
