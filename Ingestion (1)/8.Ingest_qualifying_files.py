# Databricks notebook source
# MAGIC %md
# MAGIC ###Step 1 - Ingest qualifying folder

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Read multiple json file

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

qualifying_schema = StructType(fields = [
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2- Rename and add column as required

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumn("data_source", lit(v_data_source))
final_qualifying_df = add_ingestion_date(final_qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3- Write cleaned data to Processed container

# COMMAND ----------

final_qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
