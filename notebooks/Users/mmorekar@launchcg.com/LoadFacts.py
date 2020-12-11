# Databricks notebook source
parDimDate=spark.read.parquet("/mnt/demo/Dimension/DimDate_parq.parquet")
parDimGeo=spark.read.parquet("/mnt/demo/Dimension/DimGeo_parq.parquet")
parFactCTS=spark.read.parquet("/mnt/demo/Dimension/CoronaTimeSeries.parquet")
parFactConfimed=spark.read.parquet("/mnt/demo/Dimension/ConfirmedCases.parquet")
parFactDeath=spark.read.parquet("/mnt/demo/Dimension/DeathCases.parquet")
parDimDate.createOrReplaceTempView("DimDate")
parDimGeo.createOrReplaceTempView("DimGeography")
parFactCTS.createOrReplaceTempView("FactCTS")
parFactConfimed.createOrReplaceTempView("FactConfirmedCases")
parFactDeath.createOrReplaceTempView("FactDeathCases")

# COMMAND ----------

# MAGIC %sql
# MAGIC set hive.insert.into.external.tables = true;
# MAGIC DROP TABLE Fact_CoronaTimeSeries ;
# MAGIC create external table IF NOT EXISTS Fact_CoronaTimeSeries
# MAGIC (
# MAGIC DateKey STRING NOT Null,
# MAGIC Region_Id STRING,
# MAGIC total_cases STRING,
# MAGIC new_cases STRING,
# MAGIC new_cases_smoothed STRING,
# MAGIC total_deaths STRING,
# MAGIC new_deaths STRING,
# MAGIC new_deaths_smoothed STRING,
# MAGIC total_cases_per_million STRING,
# MAGIC new_cases_per_million STRING,
# MAGIC new_cases_smoothed_per_million STRING,
# MAGIC total_deaths_per_million STRING,
# MAGIC new_deaths_per_million STRING,
# MAGIC new_deaths_smoothed_per_million STRING,
# MAGIC reproduction_rate STRING,
# MAGIC icu_patien STRING,
# MAGIC icu_patien_per_million STRING,
# MAGIC hosp_patien STRING,
# MAGIC hosp_patien_per_million STRING,
# MAGIC weekly_icu_admissions STRING,
# MAGIC weekly_icu_admissions_per_million STRING,
# MAGIC weekly_hosp_admissions STRING,
# MAGIC weekly_hosp_admissions_per_million STRING,
# MAGIC total_tes STRING,
# MAGIC new_tes STRING,
# MAGIC total_tes_per_thousand STRING,
# MAGIC new_tes_per_thousand STRING,
# MAGIC new_tes_smoothed STRING,
# MAGIC new_tes_smoothed_per_thousand STRING,
# MAGIC tes_per_case STRING,
# MAGIC positive_rate STRING,
# MAGIC tes_uni STRING,
# MAGIC stringency_index STRING,
# MAGIC population STRING,
# MAGIC population_density STRING,
# MAGIC median_age STRING,
# MAGIC aged_65_older STRING,
# MAGIC aged_70_older STRING,
# MAGIC gdp_per_capita STRING,
# MAGIC extreme_poverty STRING,
# MAGIC cardiovasc_death_rate STRING,
# MAGIC diabetes_prevalence STRING,
# MAGIC female_smokers STRING,
# MAGIC male_smokers STRING,
# MAGIC handwashing_facilities STRING,
# MAGIC hospital_beds_per_thousand STRING,
# MAGIC life_expectancy STRING,
# MAGIC human_development_index STRING
# MAGIC ) stored as parquet location '/mnt/demo/Dimension/Fact_CoronaTimeSeries.parquet';
# MAGIC 
# MAGIC 
# MAGIC INSERT into Fact_CoronaTimeSeries
# MAGIC select dt.Datekey,
# MAGIC dg.Region_Id,
# MAGIC ts.total_cases,
# MAGIC ts.new_cases,
# MAGIC ts.new_cases_smoothed,
# MAGIC ts.total_deaths,
# MAGIC ts.new_deaths,
# MAGIC ts.new_deaths_smoothed,
# MAGIC ts.total_cases_per_million,
# MAGIC ts.new_cases_per_million,
# MAGIC ts.new_cases_smoothed_per_million,
# MAGIC ts.total_deaths_per_million,
# MAGIC ts.new_deaths_per_million,
# MAGIC ts.new_deaths_smoothed_per_million,
# MAGIC ts.reproduction_rate,
# MAGIC ts.icu_patients,
# MAGIC ts.icu_patients_per_million,
# MAGIC ts.hosp_patients,
# MAGIC ts.hosp_patients_per_million,
# MAGIC ts.weekly_icu_admissions,
# MAGIC ts.weekly_icu_admissions_per_million,
# MAGIC ts.weekly_hosp_admissions,
# MAGIC ts.weekly_hosp_admissions_per_million,
# MAGIC ts.total_tests,
# MAGIC ts.new_tests,
# MAGIC ts.total_tests_per_thousand,
# MAGIC ts.new_tests_per_thousand,
# MAGIC ts.new_tests_smoothed,
# MAGIC ts.new_tests_smoothed_per_thousand,
# MAGIC ts.tests_per_case,
# MAGIC ts.positive_rate,
# MAGIC ts.tests_units,
# MAGIC ts.stringency_index,
# MAGIC ts.population,
# MAGIC ts.population_density,
# MAGIC ts.median_age,
# MAGIC ts.aged_65_older,
# MAGIC ts.aged_70_older,
# MAGIC ts.gdp_per_capita,
# MAGIC ts.extreme_poverty,
# MAGIC ts.cardiovasc_death_rate,
# MAGIC ts.diabetes_prevalence,
# MAGIC ts.female_smokers,
# MAGIC ts.male_smokers,
# MAGIC ts.handwashing_facilities,
# MAGIC ts.hospital_beds_per_thousand,
# MAGIC ts.life_expectancy,
# MAGIC ts.human_development_index
# MAGIC 
# MAGIC from DimDate dt inner join FactCTS ts on dt.Date=ts.Date
# MAGIC inner join DimGeography dg on dg.Country=ts.Country

# COMMAND ----------

# MAGIC %sql
# MAGIC set hive.insert.into.external.tables = true;
# MAGIC DROP TABLE if exists Fact_CombinedCases ;
# MAGIC create external table IF NOT EXISTS Fact_CombinedCases
# MAGIC (
# MAGIC DateKey STRING NOT Null,
# MAGIC Region_Id STRING,
# MAGIC ConfirmedCases STRING,
# MAGIC DeathCases STRING
# MAGIC )stored as parquet location '/mnt/demo/Dimension/Fact_CombinedCases.parquet';
# MAGIC 
# MAGIC 
# MAGIC With Cte_Case AS
# MAGIC (
# MAGIC Select Date,Country,State,Sum(Case) As ConfirmedCases,0 As DeathCases from FactConfirmedCases
# MAGIC group by Date,Country,State
# MAGIC union 
# MAGIC 
# MAGIC Select Date,Country,State,0 As ConfirmedCases,Sum(Case) As DeathCases from FactDeathCases
# MAGIC group by Date,Country,State
# MAGIC ) 
# MAGIC , Cte_CombinedCases AS 
# MAGIC (
# MAGIC Select Date,Case When Country="US" Then "United States" Else Country End As Country,State,Sum(ConfirmedCases) As ConfirmedCases,Sum(DeathCases) As DeathCases from Cte_Case
# MAGIC group by Date,Country,State
# MAGIC )Insert into Fact_CombinedCases
# MAGIC Select dd.Datekey,dg.Region_id,cc.ConfirmedCases,cc.DeathCases from Cte_CombinedCases cc inner Join DimGeography dg on cc.Country=dg.Country and cc.state=dg.state
# MAGIC inner Join DimDate dd on cc.Date=dd.Date

# COMMAND ----------

dbutils.notebook.exit(0)