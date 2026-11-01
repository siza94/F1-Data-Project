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

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), False),
                                StructField("raceId", IntegerType(), True),
                                StructField("driverId", IntegerType(), True),
                                StructField("constructorId", IntegerType(), True),
                                StructField("number", IntegerType(), True),
                                StructField("position", IntegerType(), True),
                                StructField("q1", StringType(), True),
                                StructField("q2", StringType(), True),
                                StructField("q3", StringType(), True)
                               ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline", True)\
.json(f"{bronze_folder_path}/{variable_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("data_source", lit(variable_data_source))\
.withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write output to processed container in Delta format

# COMMAND ----------

# overwrite_partition(final_qualifying_df, "f1_silver", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "targets.qualify_id = source.qualify_id AND targets.race_id = source.race_id"
merge_delta_data(final_qualifying_df, 'f1_silver', 'qualifying', silver_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("success")
