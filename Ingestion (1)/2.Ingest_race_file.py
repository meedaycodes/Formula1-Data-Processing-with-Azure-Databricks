# Databricks notebook source
# MAGIC %md
# MAGIC ###Step 1 - Ingest race.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2- Specify Data Schema

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

race_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 -Select only Required Columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

races_select_df = race_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Rename and Transform selected columns as Required

# COMMAND ----------

races_renamed_df = races_select_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import lit, concat , to_timestamp, current_timestamp

# COMMAND ----------

race_transformed_df_1 = add_ingestion_date(races_renamed_df)

# COMMAND ----------

race_transformed_df = race_transformed_df_1.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Drop the Date and Time column

# COMMAND ----------

race_processed_df = race_transformed_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 6 - Write the cleaned data to the Processed Container as a Delta file

# COMMAND ----------

race_processed_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
