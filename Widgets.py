# Databricks notebook source
# MAGIC %md
# MAGIC ####Prep

# COMMAND ----------

data = [(1,'Akash','Male',20000,2020),
        (2,'Ayush','Male',30000,2020),
        (3,'Mini','Female',20000,2021),
        (4,'Lipika','Female',30000,2021)
        ]
schema = ['id','name','gender','sales','year']
df = spark.createDataFrame(data,schema)

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Widgets

# COMMAND ----------

dbutils.widgets.combobox('year_cb','2020',['2020','2021'],'year_cb')

# COMMAND ----------

dbutils.widgets.dropdown(name = 'gender_dd',defaultValue='male',choices=['male','female'],label='gender')

# COMMAND ----------

dbutils.widgets.multiselect('year_ms','2020',['2020','2021'],'year_multiselect')

# COMMAND ----------

dbutils.widgets.text('text_filter','shilpa','Name')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get Values From Databricks Widgets

# COMMAND ----------

combobox_filter_value = dbutils.widgets.get("year_cb")
print(f'The combobox filter value is {combobox_filter_value}')

# COMMAND ----------

dropdown_filter_value = dbutils.widgets.get("gender_dd")
print(f'The dropdown filter value is {dropdown_filter_value}.')

# COMMAND ----------

multiselect_filter_value = dbutils.widgets.get("year_ms")
print(f'The multiselect filter value is {multiselect_filter_value}.')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Pass Widget Values in Python Code

# COMMAND ----------

from pyspark.sql.functions import  col

# COMMAND ----------

display(df.filter(col('year').isin(multiselect_filter_value.split(','))))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Delete Databricks Widgets

# COMMAND ----------

# Remove one widget
dbutils.widgets.remove("text_filter")

# COMMAND ----------

# Remove all widgets
dbutils.widgets.removeAll()
