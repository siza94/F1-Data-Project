# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake(table)
# MAGIC 4. Read data from delta lake(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1sithsaba/f1_demo'

# COMMAND ----------

results_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1sithsaba/bronze/2021-03-28/results.json")

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").save("/mnt/formula1sithsaba/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1sithsaba/f1_demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1sithsaba/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1sithsaba/demo/results_managed")

deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1sithsaba/demo/results_managed")

deltaTable.delete("points = 10")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1sithsaba/bronze/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1sithsaba/bronze/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formula1sithsaba/bronze/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge targets
# MAGIC USING f1_demo.drivers_day1 updats
# MAGIC ON targets.driverId = updats.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET targets.dob = updats.dob,
# MAGIC            targets.forename = updats.forename,
# MAGIC            targets.surname = updats.surname,
# MAGIC            targets.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (driverId, dob, forename, surname)
# MAGIC VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge targets
# MAGIC USING f1_demo.drivers_day2 updats
# MAGIC ON targets.driverId = updats.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET targets.dob = updats.dob,
# MAGIC            targets.forename = updats.forename,
# MAGIC            targets.surname = updats.surname,
# MAGIC            targets.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (driverId, dob, forename, surname)
# MAGIC VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1sithsaba/demo/drivers_merge")

deltaTable.alias("targets").merge(
    drivers_day3_df.alias("updates"),
    "targets.driverId == updates.driverId")\
    .whenMatchedUpdateUpdate(set = {"dob": "updates.dob", "forename": "updates.forename", "surname": "updates.surname", "updateDate": "current_date()"})\
    .whenNotMatchedInsert(values = 
        {
            "driverId": "updates.driverId",
            "dob": "updates.dob",
            "forename": "updates.forename",
            "surname": "updates.surname",
            "createdDate": "current_date()"
        }

    )\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo.drivers_merge VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2022-03-21T19:25:58.000+0000'

# COMMAND ----------

df = spark.read.format("delta").option("timeStampAsOf", "2021-03-28T10:00:00.000+0000").load("/mnt/formula1sithsaba/demo/drivers_merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo.drivers_merge targets
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 1
# MAGIC   ON targets.driverId = updats.driverId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transactional Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn 
# MAGIC WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1sithsaba/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.'mnt/formula1sithsaba/demo/drivers_convert_to_delta_new'

# COMMAND ----------

