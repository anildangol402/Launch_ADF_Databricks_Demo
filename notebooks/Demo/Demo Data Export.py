# Databricks notebook source
# MAGIC %md
# MAGIC #Write in CSV File Format

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
df=df.withColumnRenamed("Country/Region","Country").withColumnRenamed("Province/State","State").where("country='US'").filter("Confirmed > 0").dropDuplicates()

display(df)


# COMMAND ----------

df.write.option("header","true").csv("/mnt/demo/covidoutput/covid.csv")

# COMMAND ----------

#try to write it again
df.write.option("header","true").csv("/mnt/demo/covidoutput/covid.csv") #it should throw an error

# COMMAND ----------

df.write.option("header","true").mode('overwrite').csv("/mnt/demo/covidoutput/covid.csv") #this should overwrite

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions',2)

# COMMAND ----------

df.rdd.getNumPartitions() #this will show you how many partitions files will be crated

# COMMAND ----------

df.coalesce(1) #This will reduce the number of partitions to 1

# COMMAND ----------

# MAGIC %md
# MAGIC #Write in Parquet format

# COMMAND ----------

df.write.option("header","true").mode('overwrite').parquet("/mnt/demo/covidoutput/covid.parquet")

# COMMAND ----------

# MAGIC %md 
# MAGIC <sub> Parquet files are slower to write. However they provide great performance and extremely good read performance</sub>

# COMMAND ----------

# MAGIC %md
# MAGIC #Write in SQL Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database if not exists LaunchDemo

# COMMAND ----------

df.write.option("header","true").mode('overwrite').saveAsTable("LaunchDemo.CovidData")

# COMMAND ----------

# MAGIC %sql
# MAGIC Describe extended LaunchDemo.CovidData --Check the type of table and path

# COMMAND ----------

#Let's Create the external table
df.write.option("header","true").mode('overwrite').option("path",'/mnt/demo/covidoutput/CovidData.parquet').saveAsTable("LaunchDemo.CovidData")


# COMMAND ----------

# MAGIC %sql
# MAGIC Describe extended LaunchDemo.CovidData --Check the type of table and path

# COMMAND ----------

# MAGIC %sql
# MAGIC --Let's Drop the Table
# MAGIC Drop table LaunchDemo.CovidData

# COMMAND ----------

# MAGIC %sql
# MAGIC --Let's create the unmanaged table
# MAGIC create table if not exists  LaunchDemo.CovidData
# MAGIC using parquet
# MAGIC options 
# MAGIC (
# MAGIC path '/mnt/demo/covidoutput/CovidData.parquet'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended LaunchDemo.CovidData

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from LaunchDemo.CovidData

# COMMAND ----------

# MAGIC %md
# MAGIC #Delta Lake

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
df=df.withColumnRenamed("Country/Region","Country").withColumnRenamed("Province/State","State").where("country='US'").filter("Confirmed > 0").dropDuplicates()

display(df)

# COMMAND ----------

df.write.option("header","true").mode('overwrite').format("delta").saveAsTable("LaunchDemo.CovidDataDelta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from launchDemo.CovidDataDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC Update launchDemo.CovidData set Recovered=0 where Country='US'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update launchDemo.CovidDataDelta set Recovered=0 where Country='US'

# COMMAND ----------

