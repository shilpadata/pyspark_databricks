# Databricks notebook source
from pyspark.sql.types import *
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

# COMMAND ----------

{"id": 1, "name": "shilpa", "department": "IT"},
{"id": 2, "name": "vikas", "department": "IT"},
{"id": 3, "name": "Forest", "department": "HR", "salary": 20000}

# COMMAND ----------

line_str_data = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/line_str_data.json"
multiline_str_data = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/multiline_str_data-1.json"
line_delimited = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/line_delimited.json"
multiline = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/multiline-1.json"
corrupt_json = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/corrupt.json"

# COMMAND ----------

df1 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "PERMISSIVE")
    .load(line_str_data)
)

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "PERMISSIVE")
    .load(line_delimited)
)

# COMMAND ----------

df2.show()

# COMMAND ----------

df4 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "PERMISSIVE")
    .option("multiLine", "true")
    .load(multiline_str_data)
)

# COMMAND ----------

df4.show()

# COMMAND ----------

df3 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "PERMISSIVE")
    .option("multiLine", "true")
    .load(multiline)
)

# COMMAND ----------

df3.show()

# COMMAND ----------

df5 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "DROPMALFORMED")
    .option("multiLine", "true")
    .load(corrupt_json)
)

# COMMAND ----------

df5.count()

# COMMAND ----------

df6 = (
    spark.read.format("json")
    .option("inferschema", "true")
    .option("mode", "PERMISSIVE")
    .option("multiLine", "true")
    .load(
        "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/sample_nested_json_1.json"
    )
)
df6

# COMMAND ----------

df6.printSchema()
