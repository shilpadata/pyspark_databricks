# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creation of Dataframe

# COMMAND ----------

data = [
    (1, "Alice", "HR", 5000),
    (2, "Bob", "HR", 6000),
    (3, "Charlie", "IT", 7000),
    (4, "David", "IT", 9000),
    (5, "Eva", "Finance", 8000),
    (6, "Frank", "Finance", 5000),
    (7, "Grace", "Finance", 12000),
    (8, "Lorence", "IT", 12000),

]

# Schema definition
columns = ["EmployeeID", "EmployeeName", "Department", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####First Approach

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pyspark code using Window Function

# COMMAND ----------

# Window Function to get Avg Department salary

windowspec = Window.partitionBy("Department")
df_avgsal = df.withColumn("avg_sal", avg(col("Salary")).over(windowspec))
df_avgsal.display()

# COMMAND ----------

# Filter employees earning more than their department's average salary
df_filter = df_avgsal.filter(col("Salary") > col("avg_sal"))
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####SQL code using Window Function

# COMMAND ----------

df.createOrReplaceTempView("Emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Window Function to get Avg Department salary Using SQL
# MAGIC
# MAGIC SELECT * , AVG(Salary) OVER (PARTITION BY Department) as avg_salary FROM Emp 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filter employees earning more than their department's average salary using sql
# MAGIC
# MAGIC WITH CTE AS (
# MAGIC   SELECT * , AVG(Salary) OVER (PARTITION BY Department) as avg_salary FROM Emp 
# MAGIC )
# MAGIC SELECT * FROM CTE WHERE salary >avg_salary

# COMMAND ----------

# MAGIC %md
# MAGIC ####Second approach

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pyspark Code using Joins

# COMMAND ----------

avg_salary_df = df.groupBy("Department").agg(avg("Salary").alias("AvgSalary"))
avg_salary_df.display()

# COMMAND ----------

df_with_avg = df.join(avg_salary_df, on = "Department")
df_with_avg.display()

# COMMAND ----------

df_with_avg.filter(col("Salary") > col("Avgsalary")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####SQL Code using joins

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e1.employeeid, e1.employeename, e1.department, e1.salary,e2.avg_salary
# MAGIC  FROM EMP e1
# MAGIC JOIN 
# MAGIC (SELECT Department, AVG(salary) as avg_salary
# MAGIC FROM EMP GROUP BY Department) e2
# MAGIC ON 
# MAGIC e1.Department = e2.Department
# MAGIC where e1.salary >e2.avg_salary
