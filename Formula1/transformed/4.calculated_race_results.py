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

spark.sql("""
            MERGE INTO f1_gold.calculated_race_results tgt
            USING race_results_updated src
            ON (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id)
            WHEN MATCHED THEN
              UPDATE SET tgt.position = src.position,
                         tgt.points = src.points,
                         tgt.calculated_points = src.calculated_points,
                         tgt.updated_date = current_timestamp
            WHEN NOT MATCHED THEN
              INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
              VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_gold.calculated_race_results;
