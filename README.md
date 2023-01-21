# Formula1-Data-Processing-with-Azure-Databricks
<img width="940" alt="Image" src="https://user-images.githubusercontent.com/112330482/213871254-b5f9cb7c-97c3-4564-a7bd-aad7ba28b6c2.png">

Tools- Used: Azure Databricks, Databricks compute clusters, Databricks Notebooks, Azure Gen-2 Datalake storage, DeltaLake File format, Pyspark, Azure data Factory[Triggers, Linked Services], Power-BI
- The formula-1 historic data used for this project can be downloaded as a zipfile or using the Ergast API 
### Objectives 
  1- To develop an highly scallable data pipeline with the capability of handling both Incremental and Full refresh data load operations.
  2- Build dataframes stored delta file format that supports different queries as the need may arise
### Milestone -1
- The first step was to create an Azure Resource group with a subscription via the azure portal, all other azure resource services (databricks, storage account, Azure data factory e.t.c) used for this project was tied to the created resource group
- We then create storage account that supports the Azure Gen2 Data lake storage to house our data files, in the Blob storage database of our datalake, we created 3 containers named raw, processed and presentation. The raw container housed the raw files directly from the source (the files format was csv and json at this stage).
### Milestone -2
- We create an azure databricks instance to handle the processsing of our raw files, the raw files are partitioned by date, so at the completion of each formula1 race, a raw file containing data about the races is created, this is essential to allow for incremental load data processing
- The containers created on Azure storage explorer is then mounted on the databricks, to allow databricks access to our datalake, the functions and parameters used for this process can be found in the Set Uo folder.
- On succesfully mounting our data lake, we start ingesting our raw files
-  We need to create a compute cluster within databricks that is needed for our data processing, the compute button on the side bar allows us to create a compute cluster with specific configurations that are mostly dependent on the size of data to be processed.
```
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
```
- The code above can be viewed in the ingest_circuits_file of the ingestion folder, within this code, we defined the variable circuits_df to point to the data read from the raw folder path or raw container, the data schema is also provided as an option, the v_file_date is a notebook parameter data captures the specific date of theb file we would like to ingest and the circuits.csv specifies the name and format of the file being read
- Typical transformation performed on the read files, includes renaming of columns, inserting new columns, dropping irrelevant columns to our analysis, and also adding an igestion date column
```
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
```
- The above code shows how the cleaned data is written to the processed container, the mode (overwrite) means that everytime the circuits.csv file is cleaned, the new data would totaly replace the existing data in the table or database(f1_processed) if there's any data present before. This is an example of a FULL REFERESH LOAD
- the full refresh load operations was also used for the ingesting the race, constructors and drivers file.
### Milestone -3
- In contrast to the full refresh load opeartiion above, Incremental load operations was performed for the results, pitstops, laptimes, and qualifying files
```
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_table(final_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")
```
- The above function is called to perform incremental load operation, at the beginning of each notebook, we have included a child notebook named "includes", we can navigate to the common_functions notebook to see in full details the merge delta table function.
- At the end of the ingestion, all files are written as a table to the processed container
### Milestone- 4
- keeping our objectives in mind we need to arrrive at a set of dataframes that would enable us calculate each race results, driver standings, and constructors standing and also support other ad-hoc data request
- inside the presentation folder we,  performed several spark and sql operations (JOINS, MERGE, UPDATE, CREATE, INSERT,PARTITIONBY) to arrive at our required dataframes
-Examples of queries performed on the final dataframes can be found in the analysis folder
```
SELECT driver_name,
    COUNT(1) AS total_races,
    SUM(calculated_points) AS total_points,
    AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
 GROUP BY driver_name
 HAVING COUNT(1) >= 50
 ORDER BY total_points DESC
 ```
 - Above code would read from the f1_presentation database and navigate to the calculated_race_results Table, the output would be a table with columns driver_name, total_race counts for each driver, sum of total points for each driver, average points per race for each driver that have raced between the year 2011 and 2020, only drivers that have had more than 50 races is selected, the table is ordered according to the highest points from highest to lowest. 
 
 ### Milestone- 5
 - Following the completion of our data processing we need to align the whole process into a pipeline to allow us process new data as soon as the arrive in to our raw folder path with minimal manual intervention, enters Azure Data Factory(ADF). this is resource on created on azure that allows us to schedule our data processing needs based on specific configurations. 
- After setting up our ADF placing in our ingestion and processing databricks notebook, we used the TUMBLING WINDOW trigger type that takes in our file date as a parameter, it checks if we have the a file with the date in our raw folder path, the pipeline is trigeered if this condition is true and we can use the monitior platform on the ADF to check what stage the pipeline is running at. We can also specify what the pipeline should do if the stated condition is not met.
