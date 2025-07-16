# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the JSON file using Spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

pit_stops_schema = StructType([StructField("raceId", IntegerType(), True), 
                               StructField("driverId", IntegerType(), True), 
                               StructField("stop", IntegerType(), True), 
                               StructField("lap", IntegerType(), True), 
                               StructField("time", StringType(), True), 
                               StructField("duration", StringType(), True), 
                               StructField("milliseconds", IntegerType(), True)
                               ])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pit_stops_schema)\
.option("multiline", True)\
.json(f"{bronze_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_pit_stops_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write ouput to processed container in parquet format

# COMMAND ----------

# overwrite_partition(final_pit_stops_df, "f1_silver", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "targets.driver_id = source.driver_id AND targets.race_id = source.race_id AND targets.stop = source.stop AND targets.race_id = source.race_id"
merge_delta_data(final_pit_stops_df, 'f1_silver', 'pit_stops', silver_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")