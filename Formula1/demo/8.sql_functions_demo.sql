-- Databricks notebook source
USE f1_silver;

-- COMMAND ----------

SELECT *, concat(driver_ref, "-", code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, " ")[0] AS first_name
FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, " ")[1] AS surname
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp
FROM drivers;

-- COMMAND ----------

SELECT COUNT(*)
FROM drivers;

-- COMMAND ----------

SELECT MAX(dob)
FROM drivers;

-- COMMAND ----------

SELECT * FROM drivers WHERE dob = "2000-05-11";

-- COMMAND ----------

SELECT COUNT(*) 
FROM drivers
WHERE nationality = "British"

-- COMMAND ----------

SELECT nationality, COUNT(*) 
FROM drivers
GROUP BY nationality
ORDER BY nationality ASC;

-- COMMAND ----------

SELECT nationality, COUNT(*) 
FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality ASC;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank