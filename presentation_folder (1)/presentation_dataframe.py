# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

result_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

clean_race_df = races_df.select(col("race_year"), col("name"), col("race_timestamp"), col("circuit_id"), col("race_id"))

# COMMAND ----------

clean_race_df = clean_race_df.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

race_circuit_df = clean_race_df.join(circuits_df, clean_race_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(col("race_year"), col("race_name"), col("race_date"), col("race_id"), col("location"))

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

const_result_df = constructor_df.join(result_df, constructor_df.constructor_id == result_df.constructor_id, "left") \
.select(col("driver_id"), col("team"), col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("race_id"), col("position"))

# COMMAND ----------

driver_result_df = drivers_df.join(const_result_df, drivers_df.driver_id == const_result_df.driver_id, "left") \
.select(col("driver_name"), col("driver_number"), col("driver_nationality"), col("team"), \
         col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("race_id"))

# COMMAND ----------

clean_df_1 = race_circuit_df.join(driver_result_df, race_circuit_df.race_id == driver_result_df.race_id, "inner") \
.select( col("race_name"), col("driver_name"), col("driver_number"), col("driver_nationality"), col("team"), \
   col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("race_year"), col("position"), col("race_date"), col("location")) \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

clean_df_1.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
