-- Databricks notebook source
SELECT driver_name,
      COUNT(1) AS number_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
FROM f1_gold.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC

-- COMMAND ----------

SELECT driver_name,
      COUNT(1) AS number_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
FROM f1_gold.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC

-- COMMAND ----------

