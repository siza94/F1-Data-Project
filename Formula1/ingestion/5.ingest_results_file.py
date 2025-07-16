# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read json file using Spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")


# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-28")
variable_file_date = dbutils.widgets.get("parameter_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{bronze_folder_path}/{variable_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceID", "race_id")\
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("fastestLap", "fastest_lap")\
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("data_source", lit(variable_data_source))\
                                    .withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Drop unwanted column

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["driver_id", "race_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write output to parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_silver.results")):
#     spark.sql(f"ALTER TABLE f1_silver.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_silver.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# overwrite_partition(results_final_df, "f1_silver", "results", "race_id")


# COMMAND ----------

merge_condition = "targets.result_id = source.result_id AND targets.race_id = source.race_id"
merge_delta_data(results_deduped_df, 'f1_silver', 'results', silver_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_silver.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit("success")