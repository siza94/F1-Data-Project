-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------


SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES 

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df =  spark.read.parquet(f"{gold_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE TABLE race_results_python;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Managed Tables

-- COMMAND ----------

SELECT * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###External Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{gold_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_ext_python")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_python;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING parquet
LOCATION '/mnt/formula1sithsaba/gold/race_results_ext_sql';

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Views on Tables
-- MAGIC ####View vs Table
-- MAGIC #####Tables are supported by data & the store data
-- MAGIC #####A view is a visual representation of the data (does not stor or hold the data)- views query the data in the tables & give you results back

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW view_race_results
AS
SELECT *
FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM view_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW global_view_race_results
AS
SELECT *
FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM global_temp.global_view_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW permanent_view_race_results
AS
SELECT *
FROM demo.race_results_python
  WHERE race_year = 2005;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Why use a Permanent View?
-- MAGIC #####You have table with a lot of data that is populated by pipelines & dashboards access tables but data needs to summarized for tables, this is a great way to create permanent views

-- COMMAND ----------

