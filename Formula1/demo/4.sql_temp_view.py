# Databricks notebook source
# MAGIC %md
# MAGIC ###Access dataframe using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ####Temporary Views
# MAGIC 1. Create temp view on dataframes
# MAGIC 2. Access view from SQL cell
# MAGIC 3. Access view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df =  spark.read.parquet(f"{gold_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("view_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM view_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM view_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark SQL vs SQL Query
# MAGIC #####Spark SQL gives you the ability to put the data into a dataframe & manipulate it as needed

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global Temporary Views
# MAGIC 1. Create global temp vire on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("global_view_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.global_view_race_results 

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.global_view_race_results").show()

# COMMAND ----------

