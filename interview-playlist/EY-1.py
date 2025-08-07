# Databricks notebook source
# MAGIC %md
# MAGIC ####Find start and end location for each customer

# COMMAND ----------

# from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Sample data
data = [
    ('A001', 'Tokyo', 'Seoul'),
    ('A001', 'Singapore', 'Tokyo'),
    ('A001', 'Seoul', 'Bangkok'),
    ('A001', 'Bangkok', 'Manila'),
    ('B002', 'Paris', 'Rome'),
    ('B002', 'Berlin', 'Paris'),
    ('B002', 'Rome', 'Madrid'),
    ('C003', 'Cairo', 'Dubai'),
    ('C003', 'Istanbul', 'Cairo'),
    ('C003', 'Dubai', 'Riyadh'),
    ('C003', 'Riyadh', 'Doha'),
    ('D004', 'Toronto', 'Montreal'),
    ('D004', 'Vancouver', 'Toronto'),
    ('E005', 'Sydney', 'Melbourne')
]

# Define schema
schema = StructType([
    StructField("customer", StringType(), True),
    StructField("start_location", StringType(), True),
    StructField("end_location", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data=data, schema=schema)

print("Original DataFrame:")
df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

df_start = df.select("customer", col("start_location").alias("loc"))
df_end = df.select("customer", col("end_location").alias("loc"))
df_start.show()
df_end.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Union start and end location

# COMMAND ----------

df_union = df_start.union(df_end)
df_union.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get unique records of each customer using group and count agg

# COMMAND ----------

df_unique = df_union.groupBy("customer", "loc").agg(count(lit(1)).alias("cnt")).filter(col("cnt") == 1).drop("cnt")
df_unique.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Join df_unique and original df to get the start and end location for each customer with helpf of when otherwise condition

# COMMAND ----------

df_ans = df_unique.join(df, on = ((df.customer == df_unique.customer) 
                        & (df.start_location == df_unique.loc) | 
                        (df.end_location == df_unique.loc))
)
df_ans.show()

# COMMAND ----------

df_ans = df_ans.withColumn("cust_start_loc", 
                           when(col("loc") == col("start_location"), col("start_location"))) \
                .withColumn("cust_end_loc",
                           when(col("loc") == col("end_location"), col("end_location"))
                           ) \
                               .select(df_unique.customer, "cust_start_loc","cust_end_loc")
            

df_ans.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get the final answer by grouping data on customer col

# COMMAND ----------

df_final = df_ans.groupBy("customer").agg(min(col("cust_start_loc")).alias("start_loc") , min(col("cust_end_loc")).alias("end_loc"))
df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2: Alternative using DataFrame operations

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get start and end location 

# COMMAND ----------

# Find start points (not in end_location for same customer)
start_points = df.select("customer", "start_location").subtract(df.select("customer",col("end_location")).alias("start_location")).withColumnRenamed("start_location", "journey_start")
                                                            
# Find end points (not in start_location for same customer)  
end_points = df.select("customer", "end_location").subtract(df.select("customer",col("start_location")).alias("end_location")).withColumnRenamed("end_location", "journey_end")
start_points.show()
end_points.show()


# COMMAND ----------

# Join start and end points
result_df3 = start_points.join(end_points, "customer")

print("\nJourney Start and End Locations (DataFrame Operations):")
result_df3.show()

# COMMAND ----------

# Verify the journey paths
print("\nJourney verification:")
for customer in ['A001', 'B002', 'C003', 'D004', 'E005']:
    customer_routes = df.filter(col("customer") == customer).collect()
    print(f"\n{customer} routes:")
    for route in customer_routes:
        print(f"  {route.start_location} -> {route.end_location}")