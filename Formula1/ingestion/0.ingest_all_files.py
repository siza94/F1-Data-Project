# Databricks notebook source
results = dbutils.notebook.run("1.ingest_circuits_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("2.ingest_races_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("3.ingest_constructors_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("4.ingest_drivers_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("5.ingest_results_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("6.ingest_pitstops_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results

# COMMAND ----------

results = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"parameter_data_spurce": "Ergast API", "parameter_file_date": "2021-03-21"})

# COMMAND ----------

results