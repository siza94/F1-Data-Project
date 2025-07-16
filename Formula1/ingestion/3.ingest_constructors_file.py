# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the JSON file using the Spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")


# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{bronze_folder_path}/{variable_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename columns & add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorRef", "constructor_ref")\
                                            .withColumnRenamed("constructorId", "constructor_id")\
                                            .withColumn("load_date", current_timestamp())\
                                            .withColumn("data_source", lit(variable_data_source))\
                                            .withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.constructors

# COMMAND ----------

dbutils.notebook.exit("success")