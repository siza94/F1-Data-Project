# Databricks notebook source
# MAGIC %md
# MAGIC ###Read data required

# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{silver_folder_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{silver_folder_path}/constructors")\
.withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{silver_folder_path}/circuits")\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{silver_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{silver_folder_path}/results")\
.filter(f"file_date = '{variable_file_date}'")\
.withColumnRenamed("time", "race_time")\
.withColumnRenamed("race_id", "result_race_id")\
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id, "inner")\
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date")\
.withColumn("created_date", current_timestamp())\
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# overwrite_partition(final_df, "f1_gold", "race_results", "race_id")

# COMMAND ----------

merge_condition = "targets.race_id = source.race_id AND targets.driver_name = source.driver_name"
merge_delta_data(final_df, 'f1_gold', 'race_results', gold_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_gold.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_gold.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC