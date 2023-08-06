-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Prep

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data = [(1,'Akash','Male',20000,2020),
-- MAGIC         (2,'Ayush','Male',30000,2020),
-- MAGIC         (3,'Mini','Female',20000,2021),
-- MAGIC         (4,'Lipika','Female',30000,2021)
-- MAGIC         ]
-- MAGIC schema = ['id','name','gender','sales','year']
-- MAGIC df = spark.createDataFrame(data,schema)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("Sales")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from  sales

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.help()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Widgets

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE WIDGET DROPDOWN gender DEFAULT "Male" CHOICES SELECT DISTINCT gender FROM Sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Get Values From Databricks Widgets

-- COMMAND ----------

SELECT "${gender}"

-- COMMAND ----------

SELECT * from Sales where gender = getArgument('gender')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Delete Databricks Widgets

-- COMMAND ----------

REMOVE WIDGET gender
