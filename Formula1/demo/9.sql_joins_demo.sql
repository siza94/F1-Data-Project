-- Databricks notebook source
USE f1_gold

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW view_driver_standings_2018 AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM view_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW view_driver_standings_2020 AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM view_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inner Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Left Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
LEFT JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Right Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
RIGHT JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Full Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
FULL JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Semi Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
SEMI JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anti Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
ANTI JOIN view_driver_standings_2020
ON (view_driver_standings_2018.driver_name = view_driver_standings_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cross Join

-- COMMAND ----------

SELECT *
FROM view_driver_standings_2018
CROSS JOIN view_driver_standings_2020


-- COMMAND ----------

