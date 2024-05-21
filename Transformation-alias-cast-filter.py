# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/samples/population-vs-price/")

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header = True)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("Metrics")

# COMMAND ----------

spark.sql(""" select * from Metrics""").show()

# COMMAND ----------

spark.sql(""" select City as 2015_city from Metrics""").show()

# COMMAND ----------

df.select(col("`2014 Population estimate`").alias("2014_Population_estimate")).show()

# COMMAND ----------

df.select(col("`2014 Population estimate`").alias("2014_Population_estimate"), col("city"), col("state"),col("`state code`")).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_cast = df.select(col("`2014 rank`").cast("int").alias("2014_rank"), col("state"), col("city"),col("`state code`"))
df_cast

# COMMAND ----------

df_cast.show()

# COMMAND ----------

df_cast.filter(col("2014_rank") > 230).show()

# COMMAND ----------

df_cast.filter((col("2014_rank") > 230) & (col("2014_rank") < 235)).show()
