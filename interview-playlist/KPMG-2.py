# Databricks notebook source
# MAGIC %md
# MAGIC ####Write payspark code to interchange names of the consecutive employee. If the last entry is odd then keep the name as it is!

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [
    (1,"Alice", 1200, "Q1"),
    (2,"Bob", 900, "Q1"),
    (3,"Charlie", 1500, "Q2"),
    (4,"David", 1700, "Q2"),
    (5,"Eva", 1100, "Q3"),
    (6,"Frank", 220, "Q3"),
    (7,"Grace", 1300, "Q4")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "sales", "quarter"])
df.display()

# COMMAND ----------

##Create previous and next emp name using Lag and Lead Functionality

# COMMAND ----------

df1 = df.withColumn("previous", lag(col("name")).over(Window.orderBy(col("id")))) \
    .withColumn("next", lead(col("name")).over(Window.orderBy(col("id"))))
df1.show()

# COMMAND ----------

##Using Case when interchange the Names
df2 = df1.withColumn("interchange", when( (col("id") %2 !=0 ), coalesce(col("next"), col("name")) ) \
    .when(col("id") %2 ==0 , col("previous")))

df2.show()

# COMMAND ----------

df2.select("id","name","sales","interchange").show()