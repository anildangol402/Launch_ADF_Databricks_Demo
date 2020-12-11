# Databricks notebook source
from pyspark.sql.types import *
myschema=StructType([
  StructField('Date', TimestampType(), True),
  StructField('Country/Region', StringType(), True),
  StructField('Province/State', StringType(), True),
  StructField('Lat', StringType(), True),
  StructField('Long', StringType(), True),
  StructField('Confirmed', IntegerType(), True),
  StructField('Recovered', IntegerType(), True),
  StructField('Deaths', IntegerType(), True),
  
])
original_df = spark.read.format("csv") \
  .schema(myschema)\
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/CoronaTimeSeries.csv")
display(original_df)

# COMMAND ----------

df=original_df
display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #Let's Rename the column

# COMMAND ----------

df=df.withColumnRenamed("Country/Region","Country").withColumnRenamed("Province/State","State")
display(df)

# COMMAND ----------

display(df.select("Country").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC filter only US data

# COMMAND ----------


df=df.where("country='US'").filter("Confirmed > 0")
display(df)

# COMMAND ----------

df=df.dropDuplicates()
df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #You can combine all the columns combined. This does not guranteed sequential operation

# COMMAND ----------

df=original_df.withColumnRenamed("Country/Region","Country").withColumnRenamed("Province/State","State").where("country='US'").filter("Confirmed > 0").dropDuplicates()
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's drop a column called 'Recovered'. 

# COMMAND ----------

dfdf.drop("Recovered")
display(df)

# COMMAND ----------

df=df.drop("Recovered")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's select only few column and some deserived column

# COMMAND ----------

import pyspark.sql.functions as f
df=df.select("Date","Confirmed","Deaths").withColumn("Year",f.year("Date")).withColumn("Month",f.month("Date")).orderBy(f.col("Date").asc())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Let's Look at the Join

# COMMAND ----------

#Let's load the Death Data
covid_death_df = spark.read.format("csv") \
  .option("inferSchema","true")\
  .option("header", "true") \
  .option("sep", ",") \
  .load("/mnt/demo/Covid/2020/11/28/US_Death_Case_20201128.csv")
covid_death_df=covid_death_df.withColumnRenamed("Province/State","State").withColumnRenamed("Country/Region","Country")
cd_df=covid_death_df.alias("cd_df")

# COMMAND ----------

#Let's load the Confirmed Data
covid_confirmed_df = spark.read.format("csv") \
  .option("inferSchema","true")\
  .option("header", "true") \
  .option("sep", ",") \
  .load("/mnt/demo/Covid/2020/11/28/US_Confirmed_Case_20201128.csv")
covid_confirmed_df=covid_confirmed_df.withColumnRenamed("Province/State","State").withColumnRenamed("Country/Region","Country")
cc_df=covid_confirmed_df.alias("cc_df")

# COMMAND ----------

import pyspark.sql.functions as f
confirmed_and_death_df=cd_df.join(cc_df,(cd_df.Date==cc_df.Date) & (cd_df.State==cc_df.State),"inner")
confirmed_and_death_df=confirmed_and_death_df.select("cd_df.Date","cd_df.State",f.col("cd_df.Case").alias("Death_Case"),f.col("cc_df.Case").alias("Confirmed_Case"))

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL JOIN

# COMMAND ----------

covid_confirmed_df.createOrReplaceTempView('covid_confirmed')
covid_death_df.createOrReplaceTempView('covid_death')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace Temporary View vw_confirmed_and_death as 
# MAGIC select cd.Date,cd.State,cd.Case as Death_Case,cc.Case as Confirmed_Case from covid_confirmed as cc
# MAGIC join covid_death as cd
# MAGIC on cc.Date=cd.Date and cc.State=cd.State;
# MAGIC 
# MAGIC select * from vw_confirmed_and_death

# COMMAND ----------

#Let's access SQL view and Create the dataframe
df_confirmed_death=spark.table('vw_confirmed_and_death')
display(df_confirmed_death)

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregate and GroupBy

# COMMAND ----------

display(confirmed_and_death_df)

# COMMAND ----------

#Let's add month year and month column
confirmed_and_death_df=confirmed_and_death_df.withColumn("Year",f.year("Date")).withColumn("Month",f.month("Date"))


# COMMAND ----------

display(confirmed_and_death_df)

# COMMAND ----------



# COMMAND ----------

max_death_case_per_month=confirmed_and_death_df.groupBy("Year","Month","State").max("Death_Case").orderBy("Year","Month")
display(max_death_case_per_month)

# COMMAND ----------

