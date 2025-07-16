-- Databricks notebook source
SELECT driver_name,
      COUNT(1) AS number_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points,
      RANK() OVER (ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_gold.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 100
ORDER BY average_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW view_dominant_drivers
AS
SELECT driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points,
      RANK() OVER (ORDER BY AVG(calculated_points) DESC) AS driver_rank
FROM f1_gold.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY average_points DESC

-- COMMAND ----------

SELECT race_year, 
      driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
FROM f1_gold.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM view_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year,average_points DESC

-- COMMAND ----------

SELECT race_year, 
      driver_name,
      COUNT(1) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
FROM f1_gold.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM view_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year,average_points DESC