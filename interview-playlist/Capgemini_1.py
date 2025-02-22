# Databricks notebook source
data = [
    (1, "Sagar", 23, "Male", 68.0),
    (2, "Forest", 35, "Female", 90.0),
    (3, "Alexa", 40, "Male", 79.1),
    (4, "jennie", 40, "Female", 80.1),
    (5, "Master", 40, "Male", 90.1),
]
schema = "Id int,Name string,Age int,Gender string,Marks float"
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

'''
Output save in seperate file depending on datatypes
id, age ->Int
name, gender -> string
marks ->Float
'''

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Approach 1

# COMMAND ----------

dict = {}
for i in df.dtypes:
    if i[1] in dict.keys():
        l = dict.get(i[1])
        l.append(i[0])
        dict.update({i[1]:l})

    else:
        l = []
        l.append(i[0])
        dict.update({i[1]:l})

print(dict)
        

# COMMAND ----------

for i in dict.keys():
    df_s = df.select(dict.get(i))
    df_s.write.mode("overwrite").save(f"/Filestore/out_20feb/{i}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Approach 2

# COMMAND ----------

int_cols = [col for col,dtypes in df.dtypes if dtypes == 'int']
string_cols = [col for col,dtypes in df.dtypes if dtypes == 'string']
float_cols = [col for col,dtypes in df.dtypes if dtypes == 'float']

int_df = df.select(int_cols)
string_df = df.select(string_cols)
float_df = df.select(float_cols)

int_df.display()
string_df.display()
float_df.display()
