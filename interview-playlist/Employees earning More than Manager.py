# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Sample data
data = [
    (1, "Alice", None, 10000),  # CEO, no manager
    (2, "Bob", 1, 8000),        # Manager under Alice
    (3, "Charlie", 1, 7000),    # Manager under Alice
    (4, "David", 2, 9000),      # Employee under Bob
    (5, "Eva", 2, 6000),        # Employee under Bob
    (6, "Frank", 3, 5000),      # Employee under Charlie
    (7, "Grace", 3, 12000)      # Employee under Charlie
]

# Schema definition
columns = ["EmployeeID", "EmployeeName", "ManagerID", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pyspark approach

# COMMAND ----------

# Self-join to compare employees and their managers
join_df = df.alias("employees").join(
    df.alias("managers") , col("employees.managerid") == col("managers.employeeid") , "inner" ) \
    .select(
    col("employees.EmployeeID").alias("EmployeeID"),
    col("employees.EmployeeName").alias("EmployeeName"),
    col("employees.Salary").alias("EmployeeSalary"),
    col("managers.EmployeeName").alias("ManagerName"),
    col("managers.Salary").alias("ManagerSalary")
)
join_df.display()    

# COMMAND ----------

more_salary_than_mgr = join_df.filter(col("Employeesalary") > col("managersalary"))
more_salary_than_mgr.display()

# COMMAND ----------

more_salary_than_mgr.select(col("employeename")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####SQL Approach

# COMMAND ----------

df.createOrReplaceTempView("EMPLOYEE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT emp.employeeid, emp.employeename ,emp.salary  as emp_sal , mgr.salary as mgr_sal
# MAGIC FROM EMPLOYEE emp
# MAGIC JOIN EMPLOYEE mgr
# MAGIC ON emp.managerid =  mgr.employeeid
# MAGIC where emp.salary >mgr.salary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT emp.employeeid, emp.employeename ,emp.salary  as emp_sal , mgr.salary as mgr_sal
# MAGIC FROM EMPLOYEE emp
# MAGIC JOIN EMPLOYEE mgr
# MAGIC ON emp.managerid =  mgr.employeeid and emp.salary >mgr.salary
