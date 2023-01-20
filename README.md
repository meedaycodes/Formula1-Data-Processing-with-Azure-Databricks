# Formula1-Data-Processing-with-Azure-Databricks
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
- The containers created on Azure storage explorer is then mounted on the databricks, to allow databricks access to our datalake, the functions and parameters used for this process can be found in the 
