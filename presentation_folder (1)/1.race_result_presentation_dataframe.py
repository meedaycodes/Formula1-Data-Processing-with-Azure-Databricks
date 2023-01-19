# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")

# COMMAND ----------

constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

result_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Join circuits to race

# COMMAND ----------

clean_race_df = races_df.select(col("race_year"), col("name"), col("race_timestamp"), col("circuit_id"), col("race_id"))

# COMMAND ----------

clean_race_df = clean_race_df.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

race_circuit_df = clean_race_df.join(circuits_df, clean_race_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(col("race_year"), col("race_name"), col("race_date"), col("race_id"), col("location"), col("race_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join results to all other dataframes

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

race_results_df = result_df.join(race_circuit_df, result_df.result_race_id == race_circuit_df.race_id) \
                            .join(drivers_df, result_df.driver_id == drivers_df.driver_id)\
                            .join(constructor_df, result_df.constructor_id == constructor_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "driver_name", "driver_number", "location", "driver_nationality", "position", 
"team", "grid", "fastest_lap", "race_time", "points", "result_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("location", "circuit_location") \
.withColumnRenamed("result_file_date", "file_date")
                                 

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_table(final_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_presentation.race_results
