# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

dbutils.widgets.text("parameter_data_source", "")
variable_data_source = dbutils.widgets.get("parameter_data_source")


# COMMAND ----------

dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

new_schema = StructType(fields =[StructField("forename", StringType(), True), 
                                StructField("surname", StringType(), True),
])

# COMMAND ----------

drivers_schema = StructType(fields =[StructField("driverId", IntegerType(), False), 
                                     StructField("driverRef", StringType(), True), 
                                     StructField("number", IntegerType(), True), 
                                     StructField("code", StringType(), True), 
                                     StructField("name", new_schema, True), 
                                     StructField("dob", DateType(), True), 
                                     StructField("nationality", StringType(), True), 
                                     StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{bronze_folder_path}/{variable_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rename columns, add new columns & concatenate forename with surname

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("driverRef", "driver_ref")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                    .withColumn("data_source", lit(variable_data_source))\
                                    .withColumn("file_date", lit(variable_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write to data to datalake as parquet

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_silver.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.drivers

# COMMAND ----------

dbutils.notebook.exit("success")