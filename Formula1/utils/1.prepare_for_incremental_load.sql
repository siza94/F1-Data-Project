-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####Drop all tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_silver CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_silver
LOCATION '/mnt/formula1sithsaba/silver';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_gold CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_gold
LOCATION '/mnt/formula1sithsaba/gold';
