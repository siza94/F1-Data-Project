# Databricks notebook source
dbutils.widgets.text("parameter_file_date", "2021-03-21")
variable_file_date = dbutils.widgets.get("parameter_file_date")

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS f1_gold.calculated_race_results
          (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW race_results_updated
            AS
            SELECT races.race_year,
                constructors.name AS team_name,
                drivers.driver_id,
                drivers.name AS driver_name,
                races.race_id,
                results.position,
                results.points,
                11 - results.position AS calculated_points
            FROM f1_silver.results
            JOIN f1_silver.drivers ON (results.driver_id = drivers.driver_id)
            JOIN f1_silver.constructors ON (results.constructor_id = constructors.constructor_id)
            JOIN f1_silver.races ON (results.race_id = races.race_id)
            WHERE results.position <= 10
            AND results.file_date = '{variable_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_gold.calculated_race_results tgt
# MAGIC USING race_results_updated src
# MAGIC ON tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year
# MAGIC WHEN MATCHED THEN

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_gold.calculated_race_results
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT races.race_year,
# MAGIC       constructors.name AS team_name,
# MAGIC       drivers.name AS driver_name,
# MAGIC       results.position,
# MAGIC       results.points,
# MAGIC       11 - results.position AS calculated_points
# MAGIC FROM results
# MAGIC JOIN f1_silver.drivers ON (results.driver_id = drivers.driver_id)
# MAGIC JOIN f1_silver.constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC JOIN f1_silver.races ON (results.race_id = races.race_id)
# MAGIC WHERE results.position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_gold.calculated_race_results

# COMMAND ----------

