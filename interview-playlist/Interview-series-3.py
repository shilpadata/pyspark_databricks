# Databricks notebook source
# MAGIC %md
# MAGIC ###Deloitte Interview

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 1

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create Dataset

# COMMAND ----------

data=[(1,'John','ADF'),(1,'John','ADB'),(1,'John','PowerBI'),(2,'Steve','ADF'),(2,'Steve','SQL'),(2,'Steve','Crystal Report'),(3,'James','ADF'),(3,'James','SQL'),(3,'James','SSIS'),(4,'Acey','SQL'),(4,'Acey','SSIS'),(4,'Acey','SSIS'),(4,'Acey','ADF')]

schema=["EmpId","EmpName","Skill"]
df1=spark.createDataFrame(data,schema)
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Imports

# COMMAND ----------

from pyspark.sql.functions import collect_list,concat_ws, col, collect_set

# COMMAND ----------

df2_list = df1.groupBy(col("Empid"),col("Empname")).agg(collect_set(col("Skill")).alias("Skill_list"))
df2_list.display()

# COMMAND ----------

df3 = df2_list.select(col("empname"), concat_ws(",",col("skill_list")).alias("skills"))
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2

# COMMAND ----------

from pyspark.sql.functions import when,col, count, sum

# COMMAND ----------

data = [('IT','M'),('IT','M'),('IT','M'),('IT','F'),('HR','M'),('HR','F'),('HR','M'),('HR','F'),('HR','M'),('HR','M')]

schema = ["Deptname","Gender"]
df1 = spark.createDataFrame(data,schema)
display(df1)

# COMMAND ----------

df2 = df1.select("Deptname",when(col("gender") == "M", 1).alias("Male"), when(col("gender") == "F", 1).alias("Female"))
df2.display()

# COMMAND ----------

df2.groupBy("Deptname") \
    .agg(count("Deptname").alias("Total_emp"), \
        sum(col("Male")).alias("Total_Male"), \
        sum(col("Female")).alias("Total_FeMale") \
        ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2nd approach

# COMMAND ----------


df2 = df1.groupBy("DeptName") \
    .agg(count(when(col("gender") == "M", 1)).alias("Total_Male"), \
        count(when(col("gender") == "F", 1)).alias("Total_Female"))
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3rd Approach

# COMMAND ----------

df2 = df1.withColumn("Male_emp",when(df1["Gender"] == 'M',1).otherwise(0)) \
        .withColumn("Female_emp",when(col("Gender") == 'F',1).otherwise(0)) \
        .groupBy(col("DeptName")).agg(count("*").alias("total_employees") \
                                 ,sum("Male_emp").alias("Male_emp_count") \
                                 ,sum("Female_emp").alias("Female_emp_count") \
                                 ) \
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####SQL

# COMMAND ----------

df1.createOrReplaceTempView("Emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT deptname,
# MAGIC count(*) as Total_emp,
# MAGIC sum(case when gender == "M" then 1 else 0 end) as Total_male,
# MAGIC sum(case when gender == "F" then 1 else 0 end) as Total_Female
# MAGIC FROM emp
# MAGIC GROUP BY deptname
# MAGIC
