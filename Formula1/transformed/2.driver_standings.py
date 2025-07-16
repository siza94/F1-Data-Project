# Databricks notebook source
dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race year for which the data is to be reprocessed

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{gold_folder_path}/race_results")\
.filter(f"file_date = '{variable_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, "race_year")

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{gold_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

driver_standings_df = race_results_df\
.groupBy("race_year", "driver_name", "driver_nationality")\
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year == 2020").orderBy("total_points", ascending=False))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020").orderBy("rank"))

# COMMAND ----------

# overwrite_partition(final_df, "f1_gold", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "targets.race_year = source.race_year AND targets.driver_name = source.driver_name"
merge_delta_data(final_df, 'f1_gold', 'driver_standings', gold_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_gold.driver_standings
# MAGIC WHERE race_year = 2021