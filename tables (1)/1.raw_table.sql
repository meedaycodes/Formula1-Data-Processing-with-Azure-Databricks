-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Create Circuits table

-- COMMAND ----------

CREATE DATABASE f1_raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING, name STRING, location STRING, 
country STRING, lat DOUBLE, lng DOUBLE, alt INT, url STRING)USING csv OPTIONS (path "/mnt/formulalake/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT, year INT, round INT, circuitId INT, 
name STRING, date DATE, time STRING, url STRING)USING csv OPTIONS (path "/mnt/formulalake/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Create constructors table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, constructorRef STRING, name STRING, 
nationality STRING, url STRING)USING json OPTIONS (path "/mnt/formulalake/raw/constructors.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create drivers table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT, driverRef STRING, number INT, code STRING, name STRUCT<forename: STRING, surname: STRING>, 
dob STRING, nationality STRING, url STRING)USING json OPTIONS(path "/mnt/formulalake/raw/drivers.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Create Results Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT, raceId INT, driverId INT, constructorId INT, number INT,
grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT,
laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, 
fastestLapTime STRING, fastestLapSpeed STRING, statusId INT)USING json OPTIONS (path "/mnt/formulalake/raw/results.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Create pits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pits;
CREATE TABLE IF NOT EXISTS f1_raw.pits(
raceId INT, driverId INT, stop STRING, lap INT, time STRING, 
duration STRING, milliseconds INT)USING json OPTIONS (path "/mnt/formulalake/raw/pit_stops.json", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.pits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Lap times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.laptimes;
CREATE TABLE IF NOT EXISTS f1_raw.laptimes(
raceId INT, driverId INT, lap INT, position STRING, 
duration STRING, milliseconds INT)USING csv OPTIONS (path "/mnt/formulalake/lap_times", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Qualifying Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT, raceId INT, driverId INT, constructorId INT, 
number INT, position INT, q1 STRING, q2 STRING, q3 STRING)
USING json 
OPTIONS (path "/mnt/formulalake/raw/qualifying", multiline true)

-- COMMAND ----------

SELECT* FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying
