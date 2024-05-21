# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import *

# COMMAND ----------

path = "dbfs:/FileStore/shared_uploads/learningvideoexpert@gmail.com/dataset/json/orders_ult.json"

# COMMAND ----------

json_df = spark.read.json(path=path,multiLine=False)
json_df.display()

# COMMAND ----------

json_df.printSchema()

# COMMAND ----------

json_df.select(F.col("store.id").alias("store_id"),
               F.col("store.name").alias("store_name")).show()

# COMMAND ----------

json_df.select(F.col("payment.cardBrand").alias("cardBrand"),
               F.col("payment.cardholderName").alias("cardholderName")).show()

# COMMAND ----------

json_df.select(F.explode(F.col("items")).alias("items")).printSchema()             

# COMMAND ----------

json_df.select(F.explode(F.col("items")).alias("items")) \
    .select("items.id",
            F.col("items.productVersion").alias("product")
            ,F.col("items.productVersion.Groups").alias("Groups")) \
.printSchema()

# COMMAND ----------

json_df.select(F.explode(F.col("items")).alias("items")) \
    .select("items.id",
            F.col("items.productVersion.id").alias("product_id")
            ,F.explode(F.col("items.productVersion.Groups")).alias("Groups")) \
.printSchema()

# COMMAND ----------

json_df.select(F.explode(F.col("items")).alias("items")) \
    .select("items.id",
            F.col("items.productVersion.id").alias("product_id")
            ,F.explode(F.col("items.productVersion.Groups")).alias("Groups"), \
                F.col("items.productVersion.Groups.id").alias("id"))\
                    .show()
