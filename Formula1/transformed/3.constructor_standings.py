# Databricks notebook source
dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{gold_folder_path}/race_results")\
.filter(f"file_date = '{variable_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{gold_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df\
.groupBy("race_year", "team")\
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year == 2020").orderBy("total_points", ascending=False))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year == 2020").orderBy("rank"))

# COMMAND ----------

overwrite_partition(final_df, "f1_gold", "constructor_standings", "race_year")

# COMMAND ----------

merge_condition = "targets.race_year = source.race_year AND targets.team = source.team"
merge_delta_data(final_df, 'f1_gold', 'constructor_standings', gold_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_gold.constructor_standings
# MAGIC WHERE race_year = 2021  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM f1_gold.race_results
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC