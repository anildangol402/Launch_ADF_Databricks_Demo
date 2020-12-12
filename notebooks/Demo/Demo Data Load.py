# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC #Loading of data from internal filestore

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/CoronaTimeSeries.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Read the the data using our own schema

# COMMAND ----------


from pyspark.sql.types import *
myschema=StructType([
  StructField('Date', TimestampType(), True),
  StructField('Country', StringType(), True),
  StructField('State', StringType(), True),
  StructField('Lat', StringType(), True),
  StructField('Long', StringType(), True),
  StructField('Confirmed', IntegerType(), True),
  StructField('Recovered', IntegerType(), True),
  StructField('Deaths', IntegerType(), True),
  
])
df = spark.read.format("csv") \
  .schema(myschema)\
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/CoronaTimeSeries.csv")
display(df)


# COMMAND ----------

display(df.printSchema())

# COMMAND ----------

df.createOrReplaceTempView('demo')

# COMMAND ----------

result=spark.sql('select * from demo')


# COMMAND ----------

display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select Country,Deaths from demo 

# COMMAND ----------

# MAGIC %md
# MAGIC #Load the data from external storage

# COMMAND ----------

display(dbutils.fs.ls("/mnt/demo/"))

# COMMAND ----------

#dbutils.fs.unmount("/mnt/demo")

# COMMAND ----------

storageAccountName='launchdemoaccount'
containername='demo'
url="wasbs://"+containername+"@"+storageAccountName+".blob.core.windows.net"
dbutils.fs.mount(source=url,mount_point ="/mnt/demo",extra_configs={"fs.azure.account.key.launchdemoaccount.blob.core.windows.net":dbutils.secrets.get(scope="saskey",key="demo-datalake-sas-key")})


# COMMAND ----------

file_location = "/mnt/demo/CoronaTimeSeries.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Load directly into SQL Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists launchDemo;
# MAGIC 
# MAGIC drop table if exists launchdemo.launchDemoTable;
# MAGIC create table if not exists launchDemo.launchDemoTable
# MAGIC using 
# MAGIC csv
# MAGIC options
# MAGIC (
# MAGIC path "/mnt/demo/CoronaTimeSeries.csv",
# MAGIC header "true",
# MAGIC inferSchema "true",
# MAGIC sep ","
# MAGIC );
# MAGIC 
# MAGIC select *  from launchDemo.launchDemoTable

# COMMAND ----------

