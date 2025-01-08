# Databricks notebook source
# MAGIC %md
# MAGIC Deptname,  managername, employeename, salyear, salmonth,  totalmonthlysalary

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),
       (200,"Joanne",100,1,"13-04-23",4500),
       (200,"Joanne",100,1,"14-04-23",4020)]
schema1=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]
df_salary=spark.createDataFrame(data1,schema1)
display(df_salary)


# COMMAND ----------

#department dataframe
data2=[(1,"IT"),
       (2,"HR")]
schema2=["deptid","deptname"]
df_dept=spark.createDataFrame(data2,schema2)
display(df_dept)

# COMMAND ----------

# df1=df.join(df_dept,['deptid'])
df=df_salary.withColumn('Newsaldt',to_date('salarydt','dd-MM-yy'))
display(df)

# yyyy-MM-dd

# COMMAND ----------

df1 =df.join(df_dept , df.deptid == df_dept.deptid)
df1.display()

# COMMAND ----------

df1.alias("emp").join(df1.alias("mngr"), col("emp.mgrid") == col("mngr.empid")).display()

# COMMAND ----------

df2 = df1.alias("emp").join(df1.alias("mngr"), col("emp.mgrid") == col("mngr.empid"),'left') \
    .select(col("emp.empid"), col("emp.deptname"),
            col("emp.empname"), col("mngr.empname").alias("Managername"),
            col("emp.Newsaldt"),
            col("emp.salary"))
df2.display()

# COMMAND ----------

df3=df2.groupBy('deptname','ManagerName','EMpName',year(col('Newsaldt')).alias('Year'),date_format('Newsaldt','MMMM').alias('Month')).sum('salary') 
df3.display()
