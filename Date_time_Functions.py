# Databricks notebook source
from datetime import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####To Get Todays Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp() AS TODAYS_DATETIME

# COMMAND ----------

query = f"""select current_timestamp as cur_timestamp"""
x = spark.sql(query) 
# x.collect() 
y = (x.collect()[0][0])
print(y)

# COMMAND ----------

# MAGIC %md
# MAGIC ####To get todays date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATE() AS TODAYS_DATE

# COMMAND ----------

query = f"""select current_date as cur_date"""
curr_date = spark.sql(query).collect()[0][0]
print(curr_date)

# COMMAND ----------

todays_date = date.today()
print(todays_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ####To convert UTC to EST in SPARK SQL

# COMMAND ----------

def systime():
#     query = f"""select current_timestamp as cur_timestamp"""
    query = f"""select from_utc_timestamp(current_timestamp,'EST') as cur_timestamp_est"""
    return spark.sql(query).collect()[0][0]
print(systime())

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp() as cuur_timestamp_utc 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Datediff

# COMMAND ----------

# MAGIC %sql
# MAGIC select datediff('2009-07-31', '2009-07-30')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Date_diff

# COMMAND ----------

def date_diff(start, end,dt_part):
    query = f"""select datediff({dt_part},TIMESTAMP'{end}',TIMESTAMP'{start}') as datedifference"""
    return spark.sql(query).collect()[0][0]

# date_diff("2009-07-31", "2009-07-30","day")
# date_diff("2009-07-31 00:00:00", "2009-06-30 00:00:00","month")
display(date_diff("2011-07-31 00:00:00", "2009-06-30 00:00:00","year"))


# COMMAND ----------

# MAGIC %md
# MAGIC ####dateadd(unit, value, expr)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'march-feb' as months,dateadd(MONTH, -1, TIMESTAMP'2022-03-31 00:00:00') AS DATE_ADD
# MAGIC UNION
# MAGIC SELECT 'OCT-NOV'as months, dateadd(MONTH, 1, TIMESTAMP'2022-10-04 00:00:00') AS DATE_ADD
# MAGIC UNION
# MAGIC SELECT 'addone date', dateadd(day, 1, TIMESTAMP'2022-03-15 00:00:00')AS DATE_ADD;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dateadd(DAY, -1, TIMESTAMP'2022-03-31 00:00:00');

# COMMAND ----------

# MAGIC %md
# MAGIC ####Date_sub

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_sub('2016-07-30', 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add_months()

# COMMAND ----------

# DBTITLE 1,SQL add_months
# MAGIC %sql 
# MAGIC SELECT "2016-08-31" as Previous_date , add_months('2016-08-31', 1) AS new_date
# MAGIC UNION
# MAGIC SELECT "2016-08-31" as Previous_date ,add_months('2016-08-31', -6) AS new_date;
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Python add_months
def add_months(dt, mon_count):
    query = f"""select add_months(DATE'{dt}', {mon_count}) as new_date"""
    return spark.sql(query).collect()[0][0]
add_months('2016-08-31', -6)

# COMMAND ----------

# MAGIC %md
# MAGIC ####to_date()
# MAGIC converts string in date format yyyy-MM-dd 

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT to_date('2009-07-30 04:17:52');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date('2016-12-31', 'yyyy-MM-dd');

# COMMAND ----------

# MAGIC %md
# MAGIC ####Date_format()

# COMMAND ----------

def date_format(dt, fmt = "yyyy-MM-dd"):
    query = f"""select date_format('{dt}', '{fmt}') as fmt_dt"""
    return spark.sql(query).collect()[0][0]

# COMMAND ----------

# date_format('2009-07-30 04:17:52')
# display(date_format('2016-12-31', 'yyyy-MM-dd'))
# display(date_format('2016-12-31', 'yyyy/MM/dd'))
# display(date_format('2016-12-31', 'yy/MM/dd'))
date_format('2016-04-08', 'y')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Extract year,month,day

# COMMAND ----------

# DBTITLE 1,year()
# MAGIC %sql
# MAGIC SELECT year('2009-07-30') as year , month('2009-07-30') as month, day('2009-07-30') as day

# COMMAND ----------

# DBTITLE 1,Year()
# MAGIC %sql
# MAGIC SELECT year('2022-02-28T00:00:00.000') as year 
# MAGIC

# COMMAND ----------

# DBTITLE 1,next_day()
# MAGIC %sql
# MAGIC SELECT next_day('2024-11-05', 'TU') AS NEXT_DAY
# MAGIC UNION ALL
# MAGIC SELECT next_day('2024-11-05', 'TUESDAY')
# MAGIC UNION ALL
# MAGIC SELECT next_day('2024-11-05', 'TUE');
# MAGIC
