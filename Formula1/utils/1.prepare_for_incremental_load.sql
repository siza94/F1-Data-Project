-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Drop all tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXIST f1_silver
LOCATION '/mnt/formula1dlake/silver';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_gold CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXIST f1_gold
LOCATION '/mnt/formula1dlake/gold';
