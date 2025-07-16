# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest qualifying files

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the JSON files using Spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

qualifying_schema = StructType([StructField("qualifyingId", IntegerType(), True), 
                                StructField("raceId", IntegerType(), True), 
                                StructField("driverId", IntegerType(), True), 
                                StructField("construcorId", IntegerType(), True), 
                                StructField("number", IntegerType(), True), 
                                StructField("position", IntegerType(), True), 
                                StructField("q1", IntegerType(), True), 
                                StructField("q2", IntegerType(), True), 
                                StructField("q3", IntegerType(), True)
                               ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline", True)\
.json(f"{bronze_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write ouput to processed container in parquet format

# COMMAND ----------

# overwrite_partition(final_qualifying_df, "f1_silver", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "targets.qualify_id = source.qualify_id AND targets.race_id = source.race_id"
merge_delta_data(final_qualifying_df, 'f1_silver', 'qualifying', silver_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")