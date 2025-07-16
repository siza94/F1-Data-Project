# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Import the Data types to infer the schema

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

variable_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC #####Specify the schema & the fields

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                                     StructField("circuitRef", StringType(), True), 
                                     StructField("name", StringType(), True), 
                                     StructField("location", StringType(), True), 
                                     StructField("country", StringType(), True), 
                                     StructField("lat", DoubleType(), True), 
                                     StructField("lng", DoubleType(), True), 
                                     StructField("alt", IntegerType(), True), 
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

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{bronze_folder_path}/{variable_file_date}/circuits.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Verify that the changes to the schema have been applied

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get a view the table in the current form

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get a quick overview of the data

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop url & select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
.withColumn("data_source", lit(variable_data_source))\
.withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Add ingestion date

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Verify data has been written to the parquet file successfully

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1sithsaba/silver/circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.circuits;

# COMMAND ----------

dbutils.notebook.exit("success")