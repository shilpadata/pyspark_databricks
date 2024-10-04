# Databricks notebook source
# MAGIC %md
# MAGIC ####pyspark.sql.functions.split(str, pattern, limit=-1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Parameters:
# MAGIC
# MAGIC str – a string expression to split
# MAGIC
# MAGIC pattern – a string representing a regular expression.
# MAGIC
# MAGIC limit –an integer that controls the number of times pattern is applied.

# COMMAND ----------

from pyspark.sql.functions import split, col

# COMMAND ----------

data=data = [('Juliet','','Smith','1991-04-01'),
  ('Harry','Rose','Smith','2009-05-19'),
  ('Robert','','Williams','1979-09-05'),
  ('James','Anne','Jones','1957-11-01'),
  ('John','Mary','Brown','1990-01-17')
]

columns=["firstname","middlename","lastname","dob"]
df=spark.createDataFrame(data,columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Split Column using withColumn()

# COMMAND ----------

df_dob = df.withColumn("dob-year", split(col("dob"), "-").getItem(0)) \
        .withColumn("dob-month",split(col("dob"), "-").getItem(1) ) \
        .withColumn("dob-date",split(col("dob"), "-").getItem(2) )
df_dob.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####second method

# COMMAND ----------

import pyspark
split_col = pyspark.sql.functions.split(col("dob"), "-")

df2= df.withColumn("year", split_col.getItem(0)) \
        .withColumn("month", split_col.getItem(1)) \
        .withColumn("date", split_col.getItem(2))

df2.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using split() function of Column class- select

# COMMAND ----------

split_col = pyspark.sql.functions.split(df['dob'], '-')

df3 = df.select("firstname","lastname","dob", split_col.getItem(0).alias('year'),split_col.getItem(1).alias('month'),split_col.getItem(2).alias('day'))   
df3.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Split and drop splitted col

# COMMAND ----------

split_col = pyspark.sql.functions.split(col("dob"), "-")

df_required_col= df.withColumn("year", split_col.getItem(0)) \
        .withColumn("month", split_col.getItem(1)) \
        .withColumn("date", split_col.getItem(2)) \
        .drop(col("dob"))
df_required_col.show()
