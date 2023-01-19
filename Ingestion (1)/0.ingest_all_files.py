# Databricks notebook source
v_result = dbutils.notebook.run("1. ingest_circuits_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_race_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_drivers_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_result_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pits_stop_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_files", 0, {"p_data_source":"Ergast API", "p_file_date": "2021-03-28"})
v_result
