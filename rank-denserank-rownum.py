# Databricks notebook source
# MAGIC %md
# MAGIC ####Create Dataframe

# COMMAND ----------

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
columns= ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.display()

# COMMAND ----------

from pyspark.sql.functions import row_number,rank,dense_rank
from pyspark.sql.window import Window

# COMMAND ----------

windowSpec = Window.partitionBy("department").orderBy("Salary")

# COMMAND ----------

# MAGIC %md
# MAGIC ####row_number()

# COMMAND ----------

df.withColumn("row_number",row_number().over(windowSpec)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####rank()

# COMMAND ----------

df.withColumn("Rank",rank().over(windowSpec)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####dense_rank()

# COMMAND ----------

df.withColumn("Dense_rank",dense_rank().over(windowSpec)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Window Aggregate Functions

# COMMAND ----------

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .select("department","avg","sum","min","max") \
  .show()

# COMMAND ----------

df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .display()
