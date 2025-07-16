# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Import the Data types to infer the schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Specify the schema & the fields

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                                  StructField("year", IntegerType(), True), 
                                  StructField("round", IntegerType(), True), 
                                  StructField("circuitId", IntegerType(), True), 
                                  StructField("name", StringType(), True), 
                                  StructField("date", DateType(), True), 
                                  StructField("time", StringType(), True), 
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ######Why .option("inferSchema", True) is not used:
# MAGIC ###### 1. It runs 2 Spark Jobs (Sparks reads data & identifies schema then applies it to the DataFrame) & in production that would slow down the reads
# MAGIC ###### 2. If there is data that doesn't conform to what you're expecting, you want a failure message instaed of it carrying on to apply those changes
# MAGIC ###### On the other hand, it is okay to use with small dataset or dev environment

# COMMAND ----------

# MAGIC %md
# MAGIC #####Use schema to read data

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{bronze_folder_path}/{variable_file_date}/races.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Add ingestion date & race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
.withColumn("data_source", lit(variable_data_source))\
.withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename & keep only the required columns 

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), 
                                                   col("year").alias("race_year"), 
                                                   col("round"), 
                                                   col("circuitId").alias("circuit_id"), 
                                                   col("name"), 
                                                   col("ingestion_date"), 
                                                   col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_silver.races")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.races

# COMMAND ----------

dbutils.notebook.exit("success")