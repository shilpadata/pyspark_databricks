# Databricks notebook source
from pyspark.sql.functions import * 

# COMMAND ----------

data = [('Forest','Badminton,Cricket,Football'),
        ('Bob','Cricket,Football'),
        ('Ace','Golf,Volleyball')]
Columns = ['Name','Sports']
df = spark.createDataFrame(data,schema = Columns)
df.display()

# COMMAND ----------

df_split = df.select(col("Name"), split(col("Sports"),",").alias("Split_sport"))

# COMMAND ----------

df_split.display()

# COMMAND ----------

df_split.select(col("Name"), explode(col("Split_sport")).alias("exploded_sports")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2

# COMMAND ----------

sample_data = [("Kolkata","","WB"),
               ("","Gurgaon",None),
               (None,"","banaglore")]
columns= ["city1","city2","city3"]
df_city = spark.createDataFrame(sample_data,schema = columns)
df_city.display()

# COMMAND ----------

df_city.withColumn("City",coalesce(col("city1"), col("city2"), col("city3"))).show()

# COMMAND ----------

df_city.withColumn ("city", coalesce(when(col("city1") == "", None).otherwise(col("city1")), \
        when(col("city2") == "", None).otherwise(col("city2")), \
        when(col("city3") == "", None).otherwise(col("city3"))) \
        ).display()
