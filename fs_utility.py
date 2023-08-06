# Databricks notebook source
# MAGIC %md
# MAGIC #####mkdirs --->Create directory

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/raw_data")

# COMMAND ----------

# MAGIC %md
# MAGIC #####cp --->Copy file/folders

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/employee1.csv", 
              "/FileStore/raw_data/employee1_raw.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####ls --->Lists the contents of a directory

# COMMAND ----------

dbutils.fs.ls("/FileStore/raw_data/employee1_raw.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #####head --->Returns up to the specified maximum number bytes of the given file

# COMMAND ----------

dbutils.fs.head("/FileStore/raw_data/employee1_raw.csv", 60)

# COMMAND ----------

# MAGIC %md
# MAGIC #####mv --->Moves a file or directory,

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/call_trans.xlsx", "/FileStore/raw_data/call_trans.xlsx")


# COMMAND ----------

# MAGIC %md
# MAGIC #####put --->Writes the specified string to a file

# COMMAND ----------

dbutils.fs.put("/FileStore/raw_data/hello_db.txt", "Hello, Databricks!", True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####rm --->Removes File or Folder

# COMMAND ----------

dbutils.fs.rm("/FileStore/raw_data/hello_db.txt", True)
