# Databricks notebook source
# MAGIC %md
# MAGIC ###Step 1 - Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read multiple CSV files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Rename, add and remove column(s) as required

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

final_lap_times_df =lap_times_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("data_source", lit(v_data_source))
final_lap_times_df = add_ingestion_date(final_lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Write clean data to processed container as a parquet file

# COMMAND ----------

final_lap_times_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success")
