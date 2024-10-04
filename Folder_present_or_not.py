# Databricks notebook source
# MAGIC %md
# MAGIC ####Check weather folder is present or not
# MAGIC #####Create a UDF

# COMMAND ----------

def folder_check(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/raw_data/")

# COMMAND ----------

# path = "dbfs:/FileStore/raw_data/"
path = "dbfs:/FileStore/2020/"

if folder_check(path):
    print(f"Folder exists")
else:
    print("Folder doesnot exists")
    print("will create the folder")
    dbutils.fs.mkdirs("dbfs:/FileStore/2020/")
    print("folder Created")


# COMMAND ----------


# Clean up
dbutils.fs.rm("dbfs:/FileStore/2020/")
