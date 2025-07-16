# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the CSV files using Spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

lap_times_schema = StructType([StructField("raceId", IntegerType(), True), 
                               StructField("driverId", IntegerType(), True), 
                               StructField("lap", IntegerType(), True), 
                               StructField("position", IntegerType(), True), 
                               StructField("time", StringType(), True), 
                               StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{bronze_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_lap_time_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write ouput to processed container in parquet format

# COMMAND ----------

# overwrite_partition(final_lap_time_df, "f1_silver", "lap_times", "race_id")

# COMMAND ----------

merge_condition = "targets.driver_id = source.driver_id AND targets.race_id = source.race_id AND targets.lap = source.lap AND targets.race_id = source.race_id"
merge_delta_data(final_lap_time_df, 'f1_silver', 'lap_times', silver_folder_path, merge_condition, 'race_id')

# COMMAND ----------



# COMMAND ----------

dbutils.notebook.exit("success")