-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for csv
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create cicuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.circuits;
CREATE TABLE IF NOT EXISTS f1_bronze.circuits
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1sithsaba/bronze/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_bronze.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.races;
CREATE TABLE IF NOT EXISTS f1_bronze.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1sithsaba/bronze/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_bronze.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Simple JSON file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.constructors;
CREATE TABLE IF NOT EXISTS f1_bronze.constructors 
(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
USING json
OPTIONS(path "/mnt/formula1sithsaba/bronze/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create drivers table
-- MAGIC #####- Single line JSON file
-- MAGIC #####- Complex stucture

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.drivers;
CREATE TABLE IF NOT EXISTS f1_bronze.drivers
(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/formula1sithsaba/bronze/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create results table
-- MAGIC #####- Single line JSON
-- MAGIC #####- Simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.results;
CREATE TABLE IF NOT EXISTS f1_bronze.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId STRING
)
USING json
OPTIONS(path "/mnt/formula1sithsaba/bronze/results.json")

-- COMMAND ----------

SELECT * FROM f1_bronze.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create pit stops table
-- MAGIC #####- Multi line JSON
-- MAGIC #####- Simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.pit_stops;
CREATE TABLE IF NOT EXISTS f1_bronze.pit_stops
(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS(path "/mnt/formula1sithsaba/bronze/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_bronze.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for list of files
-- MAGIC #####- CSV file
-- MAGIC #####- Multiple files
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.lap_times;
CREATE TABLE IF NOT EXISTS f1_bronze.lap_times
(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS(path "/mnt/formula1sithsaba/bronze/lap_times")

-- COMMAND ----------

SELECT * FROM f1_bronze.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create qualifying tables
-- MAGIC #####- JSON file
-- MAGIC #####- MultiLine JSON
-- MAGIC #####- MUultiLine files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_bronze.qualifying;
CREATE TABLE IF NOT EXISTS f1_bronze.qualifying
(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raeId INT
)
USING json
OPTIONS(path "/mnt/formula1sithsaba/bronze/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_bronze.qualifying;

-- COMMAND ----------

