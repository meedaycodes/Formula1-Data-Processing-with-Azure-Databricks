# Databricks notebook source
# MAGIC %md
# MAGIC ###Step 1 - Ingest pit_stops.json file

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

pit_stops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Rename, add and remove column(s) as required

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

final_pit_stops_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("data_source", lit(v_data_source))
final_pit_stops_df = add_ingestion_date(final_pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Write clean data to processed container as a parquet file

# COMMAND ----------

final_pit_stops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")
