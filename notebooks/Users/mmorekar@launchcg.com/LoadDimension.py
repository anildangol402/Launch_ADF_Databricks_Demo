# Databricks notebook source
dbutils.widgets.text("SourceFileName", "","");
dbutils.widgets.text("SourceConfirmed", "",""); 
dbutils.widgets.text("SourceDeath", "","");

# COMMAND ----------

# File location and type
file_location=dbutils.widgets.get("SourceFileName")                           
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

df = df.withColumnRenamed("location", "Country") 
df.write.mode("overwrite").parquet('/mnt/demo/Dimension/CoronaTimeSeries.parquet')    

display(df)


# COMMAND ----------

# File location and type
dbutils.widgets.get("SourceConfirmed")
file_location=getArgument("SourceConfirmed")

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfConfirmed = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

dfConfirmed = dfConfirmed.withColumnRenamed("Country/Region", "Country")\
       .withColumnRenamed("Province/State", "State") 

dfConfirmed.write.mode("overwrite").parquet('/mnt/demo/Dimension/ConfirmedCases.parquet')                             
display(dfConfirmed)

# COMMAND ----------

# File location and type
dbutils.widgets.get("SourceDeath")
file_location=getArgument("SourceDeath")
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfDeath = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

dfDeath = dfDeath.withColumnRenamed("Country/Region", "Country")\
       .withColumnRenamed("Province/State", "State") 
dfDeath.write.mode("overwrite").parquet('/mnt/demo/Dimension/DeathCases.parquet')                          
display(dfDeath)

# COMMAND ----------

temp_table_name = "CoronaTimeSeries_csv";
temp_conf_table_name = "CoronaTimeSeriesConfirmed_csv";
temp_death_table_name = "CoronaTimeSeriesDeath_csv";

df.createOrReplaceTempView(temp_table_name);
dfConfirmed.createOrReplaceTempView(temp_conf_table_name);
dfDeath.createOrReplaceTempView(temp_death_table_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --select * from `CoronaTimeSeries_csv`;
# MAGIC 
# MAGIC set hive.insert.into.external.tables = true;
# MAGIC --Alter table DimDate_parq Set TBLPROPERTIES('external'='false')
# MAGIC Drop  table If Exists DimDate_parq ;
# MAGIC create external table IF NOT EXISTS DimDate_parq
# MAGIC (
# MAGIC DateKey STRING NOT Null,
# MAGIC Date STRING,
# MAGIC Year STRING,
# MAGIC Month STRING,
# MAGIC Day STRING,
# MAGIC Quarter STRING,
# MAGIC DayOfweek STRING,
# MAGIC DayOfYeek STRING,
# MAGIC WeekOfYear STRING
# MAGIC ) stored as parquet location '/mnt/demo/Dimension/DimDate_parq.parquet';
# MAGIC 
# MAGIC 
# MAGIC insert into DimDate_parq 
# MAGIC 
# MAGIC (Select 
# MAGIC  date_format(Date,"MDy") AS DateKey,
# MAGIC Date,
# MAGIC date_format(Date,"y") AS Year,
# MAGIC date_format(Date,"M") AS Month,
# MAGIC date_format(Date,"d") AS Day,
# MAGIC date_format(Date,"Q") AS Quarter,
# MAGIC dayofweek(Date) as DayOfweek,
# MAGIC date_format(Date,"D") AS DayOfYear,
# MAGIC weekofyear(Date) as WeekOfYear
# MAGIC from `CoronaTimeSeriesConfirmed_csv` 
# MAGIC 
# MAGIC UNION
# MAGIC Select 
# MAGIC  date_format(Date,"MDy") AS DateKey,
# MAGIC Date,
# MAGIC date_format(Date,"y") AS Year,
# MAGIC date_format(Date,"M") AS Month,
# MAGIC date_format(Date,"d") AS Day,
# MAGIC date_format(Date,"Q") AS Quarter,
# MAGIC dayofweek(Date) as DayOfweek,
# MAGIC date_format(Date,"D") AS DayOfYear,
# MAGIC weekofyear(Date) as WeekOfYear
# MAGIC from `CoronaTimeSeriesDeath_csv` ) 
# MAGIC ; 
# MAGIC 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC set hive.insert.into.external.tables = true;
# MAGIC DROP TABLE If Exists DimGeo_parq ; 
# MAGIC create external table IF NOT EXISTS DimGeo_parq
# MAGIC (
# MAGIC Region_Id STRING NOT Null,
# MAGIC ISO_Code STRING,
# MAGIC Continent STRING,
# MAGIC Country STRING,
# MAGIC State STRING
# MAGIC 
# MAGIC ) stored as parquet location '/mnt/demo/Dimension/DimGeo_parq.parquet';
# MAGIC 
# MAGIC 
# MAGIC With Cte_Geo_Base as 
# MAGIC (
# MAGIC Select Country,State from `CoronaTimeSeriesConfirmed_csv`
# MAGIC Union
# MAGIC Select Country,State from `CoronaTimeSeriesDeath_csv` 
# MAGIC Union 
# MAGIC Select Country,'' as State from `CoronaTimeSeries_csv`
# MAGIC 
# MAGIC ) 
# MAGIC , Cte_Geo_Rank as 
# MAGIC (
# MAGIC Select dense_rank() over(order by Country,State) as Region_Id,Case when Country="US" Then "United States" Else Country End As Country
# MAGIC ,State from Cte_Geo_Base
# MAGIC ) Insert into DimGeo_parq
# MAGIC select Distinct cgr.Region_Id,base.iso_code,base.continent,cgr.Country,cgr.State from Cte_Geo_Rank cgr inner join CoronaTimeSeries_csv base on cgr.Country=base.Country
# MAGIC where cgr.Country<>"World"

# COMMAND ----------

# MAGIC %md
# MAGIC <sub>Let's show the properties of the table which we created. Please Note the type of the table which indicates it is an external or unmanaged table</sub>

# COMMAND ----------

# MAGIC %md
# MAGIC <sub> Let's Recreate the table from underlyin table again</sub>

# COMMAND ----------

dbutils.notebook.exit(0)