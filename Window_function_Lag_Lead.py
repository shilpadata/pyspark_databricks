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

# prepare Data
simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("James", "Sales", 3000),    \
    ("Saif", "Sales", 4100), \
    ("Maria", "Finance", 3000),  \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
  )
columns= ["employee_name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Import functions

# COMMAND ----------

from pyspark.sql.functions import lag  ,lead
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create window

# COMMAND ----------

windowSpec  = Window.partitionBy("department").orderBy("salary")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lag()

# COMMAND ----------

# Using lag function
  
df.withColumn("lag",lag("salary").over(windowSpec)) \
      .display()

# COMMAND ----------

# Using offset    
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

# COMMAND ----------

# Using lag function with default  
df.withColumn("lag",lag("salary",2,default=100).over(windowSpec)) \
      .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lead()

# COMMAND ----------

# Using lag function
from pyspark.sql.functions import lag    
df.withColumn("lead",lead("salary").over(windowSpec)) \
      .display()

# COMMAND ----------

#With offset
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
      .show()

# COMMAND ----------

df.withColumn("lead",lead("salary",2,default=100).over(windowSpec)) \
      .show()
