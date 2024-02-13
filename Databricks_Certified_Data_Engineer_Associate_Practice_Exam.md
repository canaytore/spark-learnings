A collection of exam question samples of Databricks Certified Data Engineer Associate Exam from many sources is available here. Happy learning!

## Q1 - A data organization leader is upset about the data analysis team’s reports being different from the data engineering team’s reports. The leader believes the siloed nature of their organization’s data engineering and data analysis architectures is to blame. Which of the following describes how a data lakehouse could alleviate this issue?

A. Both teams would autoscale their work as data size evolves

**B. Both teams would use the same source of truth for their work**

C. Both teams would reorganize to report to the same department

D. Both teams would be able to collaborate on projects in real-time

E. Both teams would respond more quickly to ad-hoc requests

## Q2 - Which of the following describes a scenario in which a data team will want to utilize cluster pools?

**A. An automated report needs to be refreshed as quickly as possible.**

B. An automated report needs to be made reproducible.

C. An automated report needs to be tested to identify errors.

D. An automated report needs to be version-controlled across multiple collaborators.

E. An automated report needs to be runnable by all stakeholders.

## Q3 - Which of the following is hosted completely in the control plane of the classic Databricks architecture?

A. Worker node

B. JDBC data source

**C. Databricks web application**

D. Databricks Filesystem

E. Driver node

## Q4 - Which of the following benefits of using the Databricks Lakehouse Platform is provided by Delta Lake?

A. The ability to manipulate the same data using a variety of languages

B. The ability to collaborate in real time on a single notebook

C. The ability to set up alerts for query failures

**D. The ability to support batch and streaming workloads**

E. The ability to distribute complex data operations

## Q5 - Which of the following describes the storage organization of a Delta table?

A. Delta tables are stored in a single file that contains data, history, metadata, and other attributes.

B. Delta tables store their data in a single file and all metadata in a collection of files in a separate location.

**C. Delta tables are stored in a collection of files that contain data, history, metadata, and other attributes.**

D. Delta tables are stored in a collection of files that contain only the data stored within the table.

E. Delta tables are stored in a single file that contains only the data stored within the table.

## Q6 - Which of the following code blocks will remove the rows where the value in column age is greater than 25 from the existing Delta table my_table and save the updated table?

A. SELECT * FROM my_table WHERE age > 25;

B. UPDATE my_table WHERE age > 25;

**C. DELETE FROM my_table WHERE age > 25;**

D. UPDATE my_table WHERE age <= 25;

E. DELETE FROM my_table WHERE age <= 25;

## Q7 - A data engineer has realized that they made a mistake when making a daily update to a table. They need to use Delta time travel to restore the table to a version that is 3 days old. However, when the data engineer attempts to time travel to the older version, they are unable to restore the data because the data files have been deleted. Which of the following explains why the data files are no longer present?

**A. The VACUUM command was run on the table**

B. The TIME TRAVEL command was run on the table

C. The DELETE HISTORY command was run on the table

D. The OPTIMIZE command was nun on the table

E. The HISTORY command was run on the table

## Q8 - Which part of the Databricks Platfrom can a data engineer use to revoke permissions from users on tables?

**A. Data Explorer (Catalog Explorer)**

B. Cluster event log

C. Workspace Admin Console

D. DBFS

E. There is no way to revoke permission in Databricks platform. The data engineer needs to clone the table with the updated permissions.

## Q9 - Which of the following data lakehouse features results in improved data quality over a traditional data lake?

A. A data lakehouse provides storage solutions for structured and unstructured data.

**B. A data lakehouse supports ACID-compliant transactions.**

C. A data lakehouse allows the use of SQL queries to examine data.

D. A data lakehouse stores data in open formats.

E. A data lakehouse enables machine learning and artificial Intelligence workloads.

## Q10 - A data engineer needs to determine whether to use the built-in Databricks Notebooks versioning or version their project using Databricks Repos. Which of the following is an advantage of using Databricks Repos over the Databricks Notebooks versioning?

A. Databricks Repos automatically saves development progress

**B. Databricks Repos supports the use of multiple branches**

C. Databricks Repos allows users to revert to previous versions of a notebook

D. Databricks Repos provides the ability to comment on specific changes

E. Databricks Repos is wholly housed within the Databricks Lakehouse Platform

**Explanation:** This allows for multiple versions of a notebook or project to be developed in parallel, facilitating collaboration among team members and simplifying the process of merging changes into a single main branch.

## Q11 - A data engineer has left the organization. The data team needs to transfer ownership of the data engineer’s Delta tables to a new data engineer. The new data engineer is the lead engineer on the data team. Assuming the original data engineer no longer has access, which of the following individuals must be the one to transfer ownership of the Delta tables in Data Explorer?

A. Databricks account representative

B. This transfer is not possible

**C. Workspace administrator**

D. New lead data engineer

E. Original data engineer

## Q12 - A data analyst has created a Delta table sales that is used by the entire data analysis team. They want help from the data engineering team to implement a series of tests to ensure the data is clean. However, the data engineering team uses Python for its tests rather than SQL. Which of the following commands could the data engineering team use to access sales in PySpark?

A. SELECT * FROM sales

B. There is no way to share data between PySpark and SQL.

C. spark.sql("sales")

D. spark.delta.table("sales")

**E. spark.table("sales")**

**Explanation:** `spark.table()` function in PySpark allows access to a registered table within the SparkSession.

## Q13 - Which of the following commands will return the location of database customer360?

A. DESCRIBE LOCATION customer360;

B. DROP DATABASE customer360;

**C. DESCRIBE DATABASE customer360;**

D. ALTER DATABASE customer360 SET DBPROPERTIES ('location' = '/user'};

E. USE DATABASE customer360;

## Q14

A data engineer wants to create a new table containing the names of customers that live in France. They have written the following command:

```sql
CREATE TABLE customersInFrance
_____ AS
SELECT id, firstName, lastName,
FROM customerLocations
WHERE country = 'FRANCE';
```

A senior data engineer mentions that it is organization policy to include a table property indicating that the new table includes personally identifiable information (PII).

Which of the following lines of code fills in the above blank to successfully complete the task?

A. There is no way to indicate whether a table contains PII.

B. "COMMENT PII"

C. TBLPROPERTIES PII

**D. COMMENT "Contains PII"**

E. PII

## Q15 - Which of the following benefits is provided by the array functions from Spark SQL?

A. An ability to work with data in a variety of types at once

B. An ability to work with data within certain partitions and windows

C. An ability to work with time-related data in specified intervals

**D. An ability to work with complex, nested data ingested from JSON files**

E. An ability to work with an array of tables for procedural automation

## Q16 - Which of the following commands can be used to write data into a Delta table while avoiding the writing of duplicate records?

A. DROP

B. IGNORE

**C. MERGE**

D. APPEND

E. INSERT

**Explanation:** MERGE command is used to write data into a Delta table while avoiding the writing of duplicate records. It allows to perform an "upsert" operation, which means that it will insert new records and update existing records in the Delta table based on a specified condition. This helps maintain data integrity and avoid duplicates when adding new data to the table.

## Q17 - A data engineer needs to apply custom logic to string column city in table stores for a specific use case. In order to apply this custom logic at scale, the data engineer wants to create a SQL user-defined function (UDF). Which of the following code blocks creates this SQL UDF?

```sql
CREATE FUNCTION combine_nyc(city STRING)
RETURNS STRING
RETURN CASE 
  WHEN city = 'brooklyn' THEN 'new york'
  ELSE city
END;
```

## Q18 - A data analyst has a series of queries in a SQL program. The data analyst wants this program to run every day. They only want the final query in the program to run on Sundays. They ask for help from the data engineering team to complete this task. Which of the following approaches could be used by the data engineering team to complete this task?

A. They could submit a feature request with Databricks to add this functionality.

**B. They could wrap the queries using PySpark and use Python’s control flow system to determine when to run the final query.**

C. They could only run the entire program on Sundays.

D. They could automatically restrict access to the source table in the final query so that it is only accessible on Sundays.

E. They could redesign the data model to separate the data used in the final query into a new table.

## Q19

A data engineer runs a statement every day to copy the previous day’s sales into the table transactions. Each day’s sales are in their own file in the location "/transactions/raw".

Today, the data engineer runs the following command to complete this task:

```sql
COPY INTO transactions
FROM "/transactions/raw"
FILEFORMAT = PARQUET;
```

After running the command today, the data engineer notices that the number of records in table transactions has not changed.

Which of the following describes why the statement might not have copied any new records into the table?

A. The format of the files to be copied were not included with the FORMAT_OPTIONS keyword.

B. The names of the files to be copied were not included with the FILES keyword.

**C. The previous day’s file has already been copied into the table.**

D. The PARQUET file format does not support COPY INTO.

E. The COPY INTO statement requires the table to be refreshed to view the copied rows.

## Q20

A data engineer needs to create a table in Databricks using data from their organization’s existing SQLite database.

They run the following command:

```sql
CREATE TABKE jdbc_customer360
USING _____
OPTIONS (
  url "jdbc:sqlite:/customers.db",
  dbtable "customer360"
)
```

Which of the following lines of code fills in the above blank to successfully complete the task?

**A. org.apache.spark.sql.jdbc**

B. autoloader

C. DELTA

D. sqlite

E. org.apache.spark.sql.sqlite

## Q21 - A data engineering team has two tables. The first table march_transactions is a collection of all retail transactions in the month of March. The second table april_transactions is a collection of all retail transactions in the month of April. There are no duplicate records between the tables. Which of the following commands should be run to create a new table all_transactions that contains all records from march_transactions and april_transactions without duplicate records?

A. CREATE TABLE all_transactions AS
   SELECT * FROM march_transactions
   INNER JOIN SELECT * FROM april_transactions;
   
**B. CREATE TABLE all_transactions AS
   SELECT * FROM march_transactions
   UNION SELECT * FROM april_transactions;**
   
C. CREATE TABLE all_transactions AS
   SELECT * FROM march_transactions
   OUTER JOIN SELECT * FROM april_transactions;
   
D. CREATE TABLE all_transactions AS
   SELECT * FROM march_transactions
   INTERSECT SELECT * from april_transactions;
   
E. CREATE TABLE all_transactions AS
   SELECT * FROM march_transactions
   MERGE SELECT * FROM april_transactions;

**Explanation:** UNION [ALL | DISTINCT] --> If ALL is specified duplicate rows are preserved. --> If DISTINCT is specified the result does not contain any duplicate rows. This is the default.

## Q22 - A data engineer only wants to execute the final block of a Python program if the Python variable day_of_week is equal to 1 and the Python variable review_period is True. Which of the following control flow statements should the data engineer use to begin this conditionally executed code block?

A. if day_of_week = 1 and review_period:
B. if day_of_week = 1 and review_period = "True":
C. if day_of_week == 1 and review_period == "True":
**D. if day_of_week == 1 and review_period:**
E. if day_of_week = 1 & review_period: = "True":

## Q23

A data engineer is attempting to drop a Spark SQL table my_table. The data engineer wants to delete all table metadata and data.

They run the following command: `DROP TABLE IF EXISTS my_table`

While the object no longer appears when they run SHOW TABLES, the data files still exist.

Which of the following describes why the data files still exist and the metadata files were deleted?

A. The table’s data was larger than 10 GB

B. The table’s data was smaller than 10 GB

**C. The table was external**

D. The table did not have a location

E. The table was managed

**Explanation:** For managed tables (option E), Spark SQL manages both the metadata and the data files. When you drop a managed table, it deletes both the metadata and the associated data files, resulting in a complete removal of the table.

## Q24 - A data engineer wants to create a data entity from a couple of tables. The data entity must be used by other data engineers in other sessions. It also must be saved to a physical location. Which of the following data entities should the data engineer create?

A. Database

B. Function

C. View

D. Temporary view

**E. Table**

## Q25 - A data engineer is maintaining a data pipeline. Upon data ingestion, the data engineer notices that the source data is starting to have a lower level of quality. The data engineer would like to automate the process of monitoring the quality level. Which of the following tools can the data engineer use to solve this problem?

A. Unity Catalog

B. Data Explorer

C. Delta Lake

**D. Delta Live Tables**

E. Auto Loader

**Explanation:** Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling. Quality is explicitly mentioned in the definition.

## Q26

A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are defined against Delta Lake table sources using LIVE TABLE.

The table is configured to run in Production mode using the Continuous Pipeline Mode.

Assuming previously unprocessed data exists and all definitions are valid, what is the expected outcome after clicking Start to update the pipeline?

A. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist to allow for additional testing.

B. All datasets will be updated once and the pipeline will persist without any processing. The compute resources will persist but go unused.

**C. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will be deployed for the update and terminated when the pipeline is stopped.**

D. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated.

E. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing.

**Explanation:** Continuous Pipeline Mode in Production mode implies that the pipeline continuously processes incoming data updates at set intervals, ensuring the datasets are kept up-to-date as new data arrives. The compute resources are allocated dynamically to process and update the datasets as needed, and they will be terminated when the pipeline is stopped or shut down. This mode allows for real-time or near-real-time updates to the datasets from the streaming/live tables, ensuring that the data remains current and reflects the changes occurring in the data sources.

## Q27 - In order for Structured Streaming to reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or reprocessing, which of the following two approaches is used by Spark to record the offset range of the data being processed in each trigger?

**A. Checkpointing and Write-ahead Logs**

B. Structured Streaming cannot record the offset range of the data being processed in each trigger.

C. Replayable Sources and Idempotent Sinks

D. Write-ahead Logs and Idempotent Sinks

E. Checkpointing and Idempotent Sinks

**Explanation:** Checkpointing allows Spark to periodically save the state of the streaming application to a reliable distributed file system, which can be used for recovery in case of failures. Write-ahead logs are used to record the offset range of data being processed, ensuring that the system can recover and reprocess data from the last known offset in the event of a failure.

## Q28 - Which of the following describes the relationship between Gold tables and Silver tables?

**A. Gold tables are more likely to contain aggregations than Silver tables.**

B. Gold tables are more likely to contain valuable data than Silver tables.

C. Gold tables are more likely to contain a less refined view of data than Silver tables.

D. Gold tables are more likely to contain more data than Silver tables.

E. Gold tables are more likely to contain truthful data than Silver tables.

## Q29 - Which of the following describes the relationship between Bronze tables and raw data?

A. Bronze tables contain less data than raw data files.

B. Bronze tables contain more truthful data than raw data.

C. Bronze tables contain aggregates while raw data is unaggregated.

D. Bronze tables contain a less refined view of data than raw data.

**E. Bronze tables contain raw data with a schema applied.**

**Explanation:** Bronze tables are basically raw ingested data, often with schema borrowed from the original data source or table.

## Q30 - Which of the following tools is used by Auto Loader process data incrementally?

A. Checkpointing

**B. Spark Structured Streaming**

C. Data Explorer

D. Unity Catalog

E. Databricks SQL

## Q31

A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table.

The cade block used by the data engineer is below:

```python
(spark.table("sales")
  .withColumn("avg_price", col("sales") / col("units"))
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  ._____
  .table("new_sales")
)
```

If the data engineer only wants the query to execute a micro-batch to process data every 5 seconds, which of the following lines of code should the data engineer use to fill in the blank?

A. trigger("5 seconds")

B. trigger()

C. trigger(once="5 seconds")

**D. trigger(processingTime="5 seconds")**

E. trigger(continuous="5 seconds")

## Q32

A dataset has been defined using Delta Live Tables and includes an expectations clause:

`CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW`

What is the expected behavior when a batch of data containing data that violates these constraints is processed?

A. Records that violate the expectation are dropped from the target dataset and loaded into a quarantine table.

B. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target dataset.

**C. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.**

D. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.

E. Records that violate the expectation cause the job to fail.

## Q33 - Which of the following describes when to use the CREATE STREAMING LIVE TABLE syntax over the CREATE LIVE TABLE syntax when creating Delta Live Tables (DLT) tables using SQL?

A. CREATE STREAMING LIVE TABLE should be used when the subsequent step in the DLT pipeline is static.

**B. CREATE STREAMING LIVE TABLE should be used when data needs to be processed incrementally.**

C. CREATE STREAMING LIVE TABLE is redundant for DLT and it does not need to be used.

D. CREATE STREAMING LIVE TABLE should be used when data needs to be processed through complicated aggregations.

E. CREATE STREAMING LIVE TABLE should be used when the previous step in the DLT pipeline is static.

**Explanation:** CREATE STREAMING LIVE TABLE syntax is used to create tables that read data incrementally, while CREATE LIVE TABLE syntax is used to create tables that read data in batch mode.

## Q34 - A data engineer is designing a data pipeline. The source system generates files in a shared directory that is also used by other processes. As a result, the files should be kept as is and will accumulate in the directory. The data engineer needs to identify which files are new since the previous run in the pipeline, and set up the pipeline to only ingest those new files with each run. Which of the following tools can the data engineer use to solve this problem?

A. Unity Catalog

B. Delta Lake

C. Databricks SQL

D. Data Explorer

**E. Auto Loader**

**Explanation:** Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup.

## Q35 - Which of the following Structured Streaming queries is performing a hop from a Silver table to a Gold table?

A.
```python
(spark.readStream.load(rawSalesLocation)
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .table("newSales")
)
```

B.
```python
(spark.read.load(rawSalesLocation)
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .table("newSales")
)
```

C.
```python
(spark.table("sales")
  .withColumn("avgPrice", col("sales") / col("units"))
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .table("newSales")
)
```

D.
```python
(spark.table("sales")
  .filter(col("units") > 0)
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("append")
  .table("newSales")
)
```

**E.**
```python
(spark.table("sales")
  .groupBy("store")
  .agg(sum("sales"))
  .writeStream
  .option("checkpointLocation", checkpointPath)
  .outputMode("complete")
  .table("newSales")
)
```

## Q36 - A data engineer has three tables in a Delta Live Tables (DLT) pipeline. They have configured the pipeline to drop invalid records at each table. They notice that some data is being dropped due to quality concerns at some point in the DLT pipeline. They would like to determine at which table in their pipeline the data is being dropped. Which of the following approaches can the data engineer take to identify the table that is dropping the records?

A. They can set up separate expectations for each table when developing their DLT pipeline.

B. They cannot determine which table is dropping the records.

C. They can set up DLT to notify them via email when records are dropped.

**D. They can navigate to the DLT pipeline page, click on each table, and view the data quality statistics.**

E. They can navigate to the DLT pipeline page, click on the “Error” button, and review the present errors.

**Explanation:** These statistics often include information about records dropped, violations of expectations, and other data quality metrics.

## Q37 - A data engineer has a single-task Job that runs each morning before they begin working. After identifying an upstream data issue, they need to set up another task to run a new notebook prior to the original task. Which of the following approaches can the data engineer use to set up the new task?

A. They can clone the existing task in the existing Job and update it to run the new notebook.

**B. They can create a new task in the existing Job and then add it as a dependency of the original task.**

C. They can create a new task in the existing Job and then add the original task as a dependency of the new task.

D. They can create a new job from scratch and add both tasks to run concurrently.

E. They can clone the existing task to a new Job and then edit it to run the new notebook.

## Q38 - An engineering manager wants to monitor the performance of a recent project using a Databricks SQL query. For the first week following the project’s release, the manager wants the query results to be updated every minute. However, the manager is concerned that the compute resources used for the query will be left running and cost the organization a lot of money beyond the first week of the project’s release. Which of the following approaches can the engineering team use to ensure the query does not cost the organization any money beyond the first week of the project’s release?

A. They can set a limit to the number of DBUs that are consumed by the SQL Endpoint.

B. They can set the query’s refresh schedule to end after a certain number of refreshes.

C. They cannot ensure the query does not cost the organization money beyond the first week of the project’s release.

D. They can set a limit to the number of individuals that are able to manage the query’s refresh schedule.

**E. They can set the query’s refresh schedule to end on a certain date in the query scheduler.**

## Q39 - A data analysis team has noticed that their Databricks SQL queries are running too slowly when connected to their always-on SQL endpoint. They claim that this issue is present when many members of the team are running small queries simultaneously. They ask the data engineering team for help. The data engineering team notices that each of the team’s queries uses the same SQL endpoint. Which of the following approaches can the data engineering team use to improve the latency of the team’s queries?

A. They can increase the cluster size of the SQL endpoint.

**B. They can increase the maximum bound of the SQL endpoint’s scaling range.**

C. They can turn on the Auto Stop feature for the SQL endpoint.

D. They can turn on the Serverless feature for the SQL endpoint.

E. They can turn on the Serverless feature for the SQL endpoint and change the Spot Instance Policy to “Reliability Optimized.”

## Q40 - A data engineer wants to schedule their Databricks SQL dashboard to refresh once per day, but they only want the associated SQL endpoint to be running when it is necessary. Which of the following approaches can the data engineer use to minimize the total running time of the SQL endpoint used in the refresh schedule of their dashboard?

A. They can ensure the dashboard’s SQL endpoint matches each of the queries’ SQL endpoints.

B. They can set up the dashboard’s SQL endpoint to be serverless.

**C. They can turn on the Auto Stop feature for the SQL endpoint.**

D. They can reduce the cluster size of the SQL endpoint.

E. They can ensure the dashboard’s SQL endpoint is not one of the included query’s SQL endpoint.

## Q41 - A data engineer has been using a Databricks SQL dashboard to monitor the cleanliness of the input data to an ELT job. The ELT job has its Databricks SQL query that returns the number of input records containing unexpected NULL values. The data engineer wants their entire team to be notified via a messaging webhook whenever this value reaches 100. Which of the following approaches can the data engineer use to notify their entire team via a messaging webhook whenever the number of NULL values reaches 100?

A. They can set up an Alert with a custom template.

B. They can set up an Alert with a new email alert destination.

**C. They can set up an Alert with a new webhook alert destination.**

D. They can set up an Alert with one-time notifications.

E. They can set up an Alert without notifications.

## Q42 - A single Job runs two notebooks as two separate tasks. A data engineer has noticed that one of the notebooks is running slowly in the Job’s current run. The data engineer asks a tech lead for help in identifying why this might be the case. Which of the following approaches can the tech lead use to identify why the notebook is running slowly as part of the Job?

A. They can navigate to the Runs tab in the Jobs UI to immediately review the processing notebook.

B. They can navigate to the Tasks tab in the Jobs UI and click on the active run to review the processing notebook.

**C. They can navigate to the Runs tab in the Jobs UI and click on the active run to review the processing notebook.**

D. There is no way to determine why a Job task is running slowly.

E. They can navigate to the Tasks tab in the Jobs UI to immediately review the processing notebook.

## Q43 - A data engineer has a Job with multiple tasks that runs nightly. Each of the tasks runs slowly because the clusters take a long time to start. Which of the following actions can the data engineer perform to improve the start up time for the clusters used for the Job?

A. They can use endpoints available in Databricks SQL

B. They can use jobs clusters instead of all-purpose clusters

C. They can configure the clusters to be single-node

**D. They can use clusters that are from a cluster pool**

E. They can configure the clusters to autoscale for larger data sizes

## Q44 - A new data engineering team team has been assigned to an ELT project. The new data engineering team will need full privileges on the database customers to fully manage the project. Which of the following commands can be used to grant full permissions on the database to the new data engineering team?

A. GRANT USAGE ON DATABASE customers TO team;

B. GRANT ALL PRIVILEGES ON DATABASE team TO customers;

C. GRANT SELECT PRIVILEGES ON DATABASE customers TO teams;

D. GRANT SELECT CREATE MODIFY USAGE PRIVILEGES ON DATABASE customers TO team;

**E. GRANT ALL PRIVILEGES ON DATABASE customers TO team;**

## Q45 - Which of the following locations hosts the Databricks Web Application?

A. Data plane

**B. Control plane**

C. Databricks Filesystem

D. Databricks-managed cluster

E. Customer Cloud Account

## Q46 - According to the Databricks Lakehouse architecture, which of the following is located in the customer's cloud account?

A. Databricks Web Application

B. Notebooks

C. Repos

**D. Cluster virtual machines**

E. Workflows

## Q47 - If the default notebook language is SQL, which of the following options a data engineer can use to run a Python code in this SQL Notebook?

A. They need first to import the python module in a cell

B. This is not possible. They need to change the default language of the notebook to Python

C. Databricks detects cells language automatically, so they can write Python syntax in any cell

D. They can add %language magic command at the start of a cell to force language detection

**E. They can add %python at the start of a cell**

## Q48 - In Delta Lake tables, which of the following is the primary format for the data files?

A. Delta

**B. Parquet**

C. JSON

D. Hive-specific format

E. Both, Parquet and JSON

## Q49 - Which of the following commands can a data engineer use to compact small data files of a Delta table into larger ones?

A. PARTITION BY

B. ZORDER BY

C. COMPACT

D. VACUUM

**E. OPTIMIZE**

## Q50 - Which of the following statements is not true about Delta Lake?

A. Delta Lake provides ACID transaction guarantees

B. Delta Lake provides scalable data and metadata handling

C. Delta Lake provides audit history and time travel

**D. Delta Lake builds upon standard data formats: Parquet + XML**

E. Delta Lake supports unified streaming and batch data processing

## Q51 - How long is the default retention period of the VACUUM command?

A. 0 Days

**B. 7 Days**

C. 30 Days

D. 90 Days

E. 365 Days

## Q52 - In Databricks Repos, which of the following operations a data engineer can use to update the local version of a repo from its remote Git repository?

A. Clone

B. Commit

C. Merge

D. Push

**E. Pull**

## Q53 - A data engineer wants to create a relational object by pulling data from two tables. The relational object must be used by other data engineers in other sessions on the same cluster only. In order to save on storage costs, the data engineer wants to avoid copying and storing physical data. Which of the following relational objects should the data engineer create?

A. Temporary view

B. External table

C. Managed table

**D. Global Temporary view**

E. View

## Q54 - A new data engineering team has been assigned to work on a project. The team will need access to database customers in order to see what tables already exist. The team has its own group team. Which of the following commands can be used to grant the necessary permission on the entire database to the new team?

A. GRANT VIEW ON CATALOG customers TO team;

B. GRANT CREATE ON DATABASE customers TO team;

C. GRANT USAGE ON CATALOG team TO customers;

D. GRANT CREATE ON DATABASE team TO customers;

**E. GRANT USAGE ON DATABASE customers TO team;**

## Q55 - Which of the following commands will return the number of null values in the member_id column?

A. SELECT count(member_id) FROM my_table;

B. SELECT count(member_id) - count_null(member_id) FROM my_table;

**C. SELECT count_if(member_id IS NULL) FROM my_table;**

D. SELECT null(member_id) FROM my_table;

E. SELECT count_null(member_id) FROM my_table;

## Q56 - A data engineer needs to apply custom logic to identify employees with more than 5 years of experience in array column employees in table stores. The custom logic should create a new column exp_employees that is an array of all of the employees with more than 5 years of experience for each row. In order to apply this custom logic at scale, the data engineer wants to use the FILTER higher-order function. Which of the following code blocks successfully completes this task?

**A. SELECT store_id, employees, FILTER (employees, i -> i.years_exp > 5) AS exp_employees FROM stores;**

B. SELECT store_id, employees, FILTER (exp_employees, years_exp > 5) AS exp_employees FROM stores;

C. SELECT store_id, employees, FILTER (employees, years_exp > 5) AS exp_employees FROM stores;

D. SELECT store_id, employees, CASE WHEN employees.years_exp > 5 THEN employees ELSE NULL END AS exp_employees FROM stores;

E. SELECT store_id, employees, FILTER (exp_employees, i -> i.years_exp > 5) AS exp_employees FROM stores;

## Q57

A data engineer has a Python variable table_name that they would like to use in a SQL query. They want to construct a Python code block that will run the query using table_name.

They have the following incomplete code block: `____(f"SELECT customer_id, spend FROM {table_name}")`

Which of the following can be used to fill in the blank to successfully complete the task?

A. spark.delta.sql

B. spark.delta.table

C. spark.table

D. dbutils.sql

**E. spark.sql**

## Q58

A data engineer has created a new database using the following command: `CREATE DATABASE IF NOT EXISTS customer360;`

In which of the following locations will the customer360 database be located?

A. dbfs:/user/hive/database/customer360

B. dbfs:/user/hive/warehouse

**C. dbfs:/user/hive/customer360**

D. More information is needed to determine the correct response

E. dbfs:/user/hive/database

## Q59

A data engineer is attempting to drop a Spark SQL table my_table and runs the following command: `DROP TABLE IF EXISTS my_table;`

After running this command, the engineer notices that the data files and metadata files have been deleted from the file system.

Which of the following describes why all of these files were deleted?

**A. The table was managed**

B. The table's data was smaller than 10 GB

C. The table's data was larger than 10 GB

D. The table was external

E. The table did not have a location

## Q60 - In which of the following scenarios should a data engineer use the MERGE INTO command instead of the INSERT INTO command?

A. When the location of the data needs to be changed

B. When the target table is an external table

C. When the source table can be deleted

**D. When the target table cannot contain duplicate records**

E. When the source is not a Delta table

## Q61

A data engineer needs to create a table in Databricks using data from a CSV file at location /path/to/csv.

They run the following command:

```sql
CREATE TABLE new_table
_____
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "path/to/csv"
```

Which of the following lines of code fills in the above blank to successfully complete the task?

A. None of these lines of code are needed to successfully complete the task

**B. USING CSV**

C. FROM CSV

D. USING DELTA

E. FROM "path/to/csv"

## Q62

A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table.

The code block used by the data engineer is below:

```python
(spark.readStream
   .table("sales")
   .withColumn("avg_proce", col("sales") / col("units"))
   .writeStream
   .option("checkpointLocation", checkpointPath)
   .outputMode("complete")
   .____,
   .table("new_sales")
)
```

If the data engineer only wants the query to process all of the available data in as many batches as required, which of the following lines of code should the data engineer use to fill in the blank?

A. processingTime(1)

**B. trigger(availableNow=True)**

C. trigger(parallelBatch=True)

D. trigger(processingTime="once")

E. trigger(continuous="once")

## Q63 - A data engineer has developed a data pipeline to ingest data from a JSON source using Auto Loader, but the engineer has not provided any type inference or schema hints in their pipeline. Upon reviewing the data, the data engineer has noticed that all of the columns in the target table are of the string type despite some of the fields only including float or boolean values. Which of the following describes why Auto Loader inferred all of the columns to be of the string type?

A. There was a type mismatch between the specific schema and the inferred schema

**B. JSON data is a text-based format**

C. Auto Loader only works with string data

D. All of the fields had at least one null value

E. Auto Loader cannot infer the schema of ingested data

## Q64

A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are defined against Delta Lake table sources using LIVE TABLE.

The table is configured to run in Development mode using the Continuous Pipeline Mode.

Assuming previously unprocessed data exists and all definitions are valid, what is the expected outcome after clicking Start to update the pipeline?

A. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated.

B. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist until the pipeline is shut down.

C. All datasets will be updated once and the pipeline will persist without any processing. The compute resources will persist but go unused.

D. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing.

**E. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist to allow for additional testing.**

## Q65 - Which of the following data workloads will utilize a Gold table as its source?

A. A job that enriches data by parsing its timestamps into a human-readable format

B. A job that aggregates uncleaned data to create standard summary statistics

C. A job that cleans data by removing malformatted records

**D. A job that queries aggregated data designed to feed into a dashboard**

E. A job that ingests raw data from a streaming source into the Lakehouse

## Q66

A data engineer has joined an existing project and they see the following query in the project repository:

```sql
CREATE STREAMING LIVE TABLE loyal_customers AS
SELECT customer_id
FROM STREAM(LIVE.customers)
WHERE loyalty_level = 'high';
```

Which of the following describes why the STREAM function is included in the query?

A. The STREAM function is not needed and will cause an error.

B. The table being created is a live table.

**C. The customers table is a streaming live table.**

D. The customers table is a reference to a Structured Streaming query on a PySpark DataFrame.

E. The data in the customers table has been updated since its last run.

## Q67 - Which of the following describes a benefit of a data lakehouse that is unavailable in a traditional data warehouse?

A. A data lakehouse provides a relational system of data management.

B. A data lakehouse captures snapshots of data for version control purposes.

C. A data lakehouse couples storage and compute for complete control. 

D. A data lakehouse utilizes proprietary storage formats for data.

**E. A data lakehouse enables both batch and streaming analytics.**

## Q68 - Which of the following locations hosts the driver and worker nodes of a Databricks-managed cluster?

**A. Data plane**

B. Control plane

C. Databricks Filesystem

D. JDBC data source

E. Databricks web application

## Q69 - A data architect is designing a data model that works for both video-based machine learning workloads and highly audited batch ETL/ELT workloads. Which of the following desctibes how using a data lakehouse can help the data architect meet the needs of both workloads?

A. A data lakehouse requires very little data modeling.

B. A data lakehouse combines compute and storage for simple governance.

C. A data lakehouse provides autoscaling for compute clusters.

**D. A data lakehouse stores unstructured data and is ACID-complaint.**

E. A data lakehouse fully exists in the cloud.

## Q70 - Which of the following describes a scenario in which a data engineer will want to use a Job cluster instead of an all-purpose cluster?

A. An ad-hoc analytics report needs to be developed while minimizing compute costs.

B. A data team needs to collaborate on the development of a machine learning model.

**C. An automated workflow needs to be run every 30 minutes.**

D. A Databricks SQL query needs to be scheduled for upward reporting.

E. A data engineer needs to manually investigate a production error.

## Q71 - A data engineer has created a Delta table as part of a data pipeline. Downstream data analysts now need SELECT permission on the Delta table. Assuming the data engineer is the Delta table owner, which part of the Databricks Lakehouse Platform can the data engineer use to grant the data analysts the appripriate access?

A. Repos

B. Jobs

**C. Data Explorer**

D. Databricks Filesystem

E. Dashboards

## Q72 - Two junior data engineers are authoring separate parts of a single data pipeline notebook. They are working on separate Git branches so they can pair program on the same notebook simultaneously. A senior data engineer experienced in Databricks suggests there is a better alternative for this type of collabration. Which of the following supports the senior data engineer's claim?

A. Databricks Notebooks support automatic change-tracking and versioning

**B. Databricks Notebooks support real-time coauthoring on a single notebook**

C. Databricks Notebooks support support commenting and notification comments

D. Databricks Notebooks support the use of multiple languages in the same notebook

E. Databricks Notebooks support the creation of interactive data visualizations

## Q73 - Which of the following describes how Databricks Repos can help facilitate CI/CD workflows on the Databricks Lakehouse Platform?

A. Databricks Repos can facilitate the pull request, review, and approval process before merging branches

B. Databricks Repos can merge changes from a secondary Git branch into a main Git branch

C. Databricks Repos can be used to design, develop, and trigger Git automation pipelines

D. Databricks Repos can store the single-source-of-truth Git repository

**E. Databricks Repos can commit or push code changes to trigger a CI/CD process**

## Q74 - Which of the following statements describes Delta Lake?

A. Delta Lake is an open source analytics engine used for big data workloads.

**B. Delta Lake is an open format storage layer that delivers reliability, security, and performance.**

C. Delta Lake is an open source platform to help manage the complete machine learning lifecycle.

D. Delta Lake is an open source data storage format for distributed data.

E. Delta Lake is an open format storage layer that processes data.

## Q75 - Which of the following approaches can the data engineer use to obtain a version-controllable configuration of the Job's schedule and configuration?

A. They can link the job to notebooks that are part of a Databricks Repo

B. They can submit the job once on a Job Cluster

**C. They can download the JSON equivalent of the job from the Job's page

D. They can submit the Job once on a All-Purpose Cluster

E. They can download the XML description of the job from the Job's page

## Q76 - A data analyst has noticed that their Databricks SQL queries are running too slowly. They claim that this issue is affecting all of their sequentially run queries. They ask the data engineering team for help. The data engineering team notices that each of the queries uses the same SQL endpoint, but the SQL endpoint is not used by any other user. Which of the following approaches can the data engineering team use to improve the latency of the data analyst’s queries?

**A. They can increase the cluster size of the SQL endpoint.**

B. They can increase the maximum bound of the SQL endpoint’s scaling range.

C. They can turn on the Auto Stop feature for the SQL endpoint.

D. They can turn on the Serverless feature for the SQL endpoint.

E. They can turn on the Serverless feature for the SQL endpoint and change the Spot Instance Policy to “Reliability Optimized.”

## Q77 - A data engineering team needs to query a Delta table to extract rows that all meet the same condition. However, the team has noticed that the query is running slowly. The team has already tuned the size of the data files. Upon investigating, the team has concluded that the rows meeting the condition are sparsely located throughout each of the data files. Based on the scenario, which of the following optimization techniques could speed up the query?

A. Data Skipping

**B. Z-Ordering**

C. Bin-Packing

D. Write as a Parquet File

E. Tuning File Size

## Q78

You have two tables, one is a delta table named conveniently enough as “delta_table” and the other is a parquet table named once again quite descriptively as parquet_table. Some error in ETL upstream has led to source_table having zero records, when it is supposed to have new records generated daily. If I run the following statements.

```sql
Insert overwrite delta_table select * from source_table;
Insert overwrite parquet_table select * from source table;
```

Which statement below is correct?

A. Both tables can be restored using “Restore table table_name version as of <previous \version>

B. Both tables, delta_table and parquet_table have been completed deleted, with no options to restore

**C. The current version of the delta table is a full replacement of the previous version, but it can be recovered through time travel or a restore statement**

D. If the table is an external table the data is recoverable for the parquet table

## Q79 - An engineering manager uses a Databricks SQL query to monitor their team’s progress on fixes related to customer-reported bugs. The manager checks the results of the query every day, but they are manually rerunning the query each day and waiting for the results. Which of the following approaches can the manager use to ensure the results of the query are updated each day?

A. They can schedule the query to run every 1 day from the Jobs UI.
**B. They can schedule the query to refresh every 1 day from the query’s page in Databricks SQL.**
C. They can schedule the query to run every 12 hours from the Jobs UI.
D. They can schedule the query to refresh every 1 day from the SQL endpoint’s page in Databricks SQL.
E. They can schedule the query to refresh every 12 hours from the SQL endpoint’s page in Databricks SQL.

## Q80 - You have written a notebook to generate a summary data set for reporting, Notebook was scheduled using the job cluster, but you realized it takes 8 minutes to start the cluster, what feature can be used to start the cluster in a timely fashion so your job can run immediately?

A. Setup an additional job to run ahead of the actual job so the cluster is running when the second job starts

**B. Use the Databricks cluster pool feature to reduce the startup time**

C. Use Databricks Premium Edition instead of Databricks Standard Edition

D. Pin the cluster in the Cluster UI page so it is always available to the jobs

E. Disable auto termination so the cluster is always running.

## Q81 - Data engineering team has provided 10 queries and asked Data Analyst team to build a dashboard and refresh the data every day at 8 AM, identify the best approach to set up data refresh for this dashboard? Which of the following approaches can the manager use to ensure the results of the query are updated each day?

A. Each query requires a separate task and setup 10 tasks under a single job to run at 8 AM to refresh the dashboard

**B. The entire dashboard with 10 queries can be refreshed at once, single schedule needs to be setup to refresh at 8 AM.**

C. Setup Job with Linear Dependency to load all 10 queries into a table so the dashboard can be refreshed at once.

D. A Dashboard can only refresh one query at a time, 10 schedules to set up the refresh.

E. Use Incremental refresh to run at 8 AM every day

## Q82 - Which of the following SQL keywords can be used to append new rows to an existing Delta table?

A. UPDATE

B. COPY

**C. INSERT INTO**

D. DELETE

E. UNION

## Q83 - A data engineer needs to create a database called customer360 at the location /customer/customer360. The data engineer is unsure if one of their colleagues has already created the database. Which of the following commands should the data engineer run to complete this task?

A. CREATE DATABASE customer360 LOCATION '/customer/customer360';

B. CREATE DATABASE IF NOT EXISTS customer360;

**C. CREATE DATABASE IF NOT EXISTS customer360 LOCATION '/customer/customer360';**

D. CREATE DATABASE IF NOT EXISTS customer360 DELTA LOCATION '/customer/customer360';

E. CREATE DATABASE customer360 DELTA LOCATION '/customer/customer360';

## Q84 - A junior data engineer needs to create a Spark SQL table my_table for which Spark manages both the data and the metadata. The metadata and data should also be stored in the Databricks Filesystem (DBFS). Which of the following commands should a senior data engineer share with the junior data engineer to complete this task?

A. CREATE TABLE my_table (id STRING, value STRING) USING org.apache.spark.sql.parquet OPTIONS (PATH "storage-path");

B. CREATE MANAGED TABLE my_table (id STRING, value STRING) USING org.apache.spark.sql.parquet OPTIONS (PATH "storage-path");

C. CREATE MANAGED TABLE my_table (id STRING, value STRING);

D. CREATE TABLE my_table (id STRING, value STRING) USING DBFS;

**E. CREATE TABLE my_table (id STRING, value STRING);**

## Q85 - A data engineer wants to create a relational object by pulling data from two tables. The relational object must be used by other data engineers in other sessions. In order to save on storage costs, the data engineer wants to avoid copying and storing physical data. Which of the following relational objects should the data engineer create?

**A. View**

B. Temporary view

C. Delta Table

D. Database

E. Spark SQL Table

## Q86 - A data engineering team has created a series of tables using Parquet data stored in an external system. The team is noticing that after appending new rows to the data in the external system, their queries within Databricks are not returning the new rows. They identify the caching of the previous data as the cause of this issue. Which of the following approaches will ensure that the data returned by queries is always up-to-date?

**A. The tables should be converted to the Delta format**

B. The tables should be stored in a cloud-based external system

C. The tables should be refreshed in the writing cluster before the next query is run

D. The tables should be altered to include metadata to not cache

E. The tables should be updated before the next query is run

## Q87

A table customerLocations exists with the following schema:

```
id STRING,
date STRING,
city STRING,
country STRING
```

A senior data engineer wants to create a new table from this table using the following command:

```sql
CREATE TABLE customersPerCountry AS
SELECT country,
COUNT(*) AS customers
FROM customerLocations
GROUP BY country;
```

A junior data engineer asks why the schema is not being declared for the new table.

Which of the following responses explains why declaring the schema is not necessary?

**A. CREATE TABLE AS SELECT statements adopt schema details from the source table and query.**

B. CREATE TABLE AS SELECT statements infer the schema by scanning the data.

C. CREATE TABLE AS SELECT statements result in tables where schemas are optional.

D. CREATE TABLE AS SELECT statements assign all columns the type STRING.

E. CREATE TABLE AS SELECT statements result in tables that do not support schemas.

## Q88 - A data engineer is overwriting data in a table by deleting the table and recreating the table. Another data engineer suggests that this is inefficient and the table should simply be overwritten instead. Which of the following reasons to overwrite the table instead of deleting and recreating the table is *incorrect*?

A. Overwriting a table is efficient because no files need to be deleted.

**B. Overwriting a table results in a clean table history for logging and audit purposes.**

C. Overwriting a table maintains the old version of the table for Time Travel.

D. Overwriting a table is an atomic operation and will not leave the table in an unfinished state.

E. Overwriting a table allows for concurrent queries to be completed while in progress.

## Q89 - Which of the following commands will return records from an existing Delta table my_table where duplicates have been removed?

A. DROP DUPLICATES FROM my_table;

B. SELECT * FROM my_table WHERE duplicate = False;

**C. SELECT DISTINCT * FROM my_table;**

D. MERGE INTO my_table a USING new_records b ON a.id = b.id WHEN NOT MATCHED THEN INSERT *;

E. MERGE INTO my_table a USING new_records b;

## Q90 - A data engineer wants to horizontally combine two tables as a part of a query. They want to use a shared column as a key column, and they only want the query result to contain rows whose value in the key column is present in both tables. Which of the following SQL commands can they use to accomplish this task?

**A. INNER JOIN**

B. OUTER JOIN

C. LEFT JOIN

D. MERGE

E. UNION

## Q91

A junior data engineer has ingested a JSON file into a table raw_table with the following schema:

```
cart_id STRING,
items ARRAY<item_id:STRING>
```

The junior data engineer would like to unnest the items column in raw_table to result in a new table with the following schema:

```
cart_id STRING,
item_id STRING
```

Which of the following commands should the junior data engineer run to complete this task?

A. SELECT cart_id, filter(items) AS item_id FROM raw_table;

B. SELECT cart_id, flatten(items) AS item_id FROM raw_table;

C. SELECT cart_id, reduce(items) AS item_id FROM raw_table;

**D. SELECT cart_id, explode(items) AS item_id FROM raw_table;**

E. SELECT cart_id, slice(items) AS item_id FROM raw_table;

## Q92

A data engineer has ingested a JSON file into a table raw_table with the following schema:

```
transaction_id STRING,
payload ARRAY<customer_id:STRING, date:TIMESTAMP, store_id:STRING>
```

The data engineer wants to efficiently extract the date of each transaction into a table with the following schema:

```
transaction_id STRING,
date TIMESTAMP
```

Which of the following commands should the data engineer run to complete this task?

A. SELECT transaction_id, explode(payload) FROM raw_table;

**B. SELECT transaction_id, payload.date FROM raw_table;**

C. SELECT transaction_id, date FROM raw_table;

D. SELECT transaction_id, payload[date] FROM raw_table;

E. SELECT transaction_id, date from payload FROM raw_table;

## Q93

A data analyst has provided a data engineering team with the following Spark SQL query:

```sql
SELECT district,
avg(sales)
FROM store_sales_20220101
GROUP BY district;
```

The data analyst would like the data engineering team to run this query every day. The date at the end of the table name (20220101) should automatically be replaced with the current date each time the query is run.

Which of the following approaches could be used by the data engineering team to efficiently automate this process?

**A. They could wrap the query using PySpark and use Python’s string variable system to automatically update the table name.**

B. They could manually replace the date within the table name with the current day’s date.

C. They could request that the data analyst rewrites the query to be run less frequently.

D. They could replace the string-formatted date in the table with a timestamp-formatted date.

E. They could pass the table into PySpark and develop a robustly tested module on the existing query.

## Q94 - A data engineer has ingested data from an external source into a PySpark DataFrame raw_df. They need to briefly make this data available in SQL for a data analyst to perform a quality assurance check on the data. Which of the following commands should the data engineer run to make this data available in SQL for only the remainder of the Spark session?

**A. raw_df.createOrReplaceTempView("raw_df")**

B. raw_df.createTable("raw_df")

C. raw_df.write.save("raw_df")

D. raw_df.saveAsTable("raw_df")

E. There is no way to share data between PySpark and SQL.

## Q95

A data engineer needs to dynamically create a table name string using three Python variables: region, store, and year. An example of a table name is below when region = "nyc", store = "100", and year = "2021": `nyc100_sales_2021`

Which of the following commands should the data engineer use to construct the table name in Python?

A. "{region}+{store}+_sales_+{year}"

B. f"{region}+{store}+_sales_+{year}"

C. "{region}{store}_sales_{year}"

**D. f"{region}{store}_sales_{year}"**

E. {region}+{store}+"_sales_"+{year}

## Q96

A data engineer has developed a code block to perform a streaming read on a data source. The code block below is returning an error:

```python
(spark
.read
.schema(schema)
.format("cloudFiles")
.option("cloudFiles.format", "json")
.load(dataSource)
)
```

Which of the following changes should be made to the code block to configure the block to successfully perform a streaming read?

**A. The .read line should be replaced with .readStream.**

B. A new .stream line should be added after the .read line.

C. The .format("cloudFiles") line should be replaced with .format("stream").

D. A new .stream line should be added after the spark line.

E. A new .stream line should be added after the .load(dataSource) line.

## Q97

A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table.

The code block used by the data engineer is below:

```python
(spark.table("sales")
.withColumn("avg_price", col("sales") / col("units"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("complete")
._____
.table("new_sales")
)
```

If the data engineer only wants the query to execute a single micro-batch to process all of the available data, which of the following lines of code should the data engineer use to fill in the blank?

**A. trigger(once=True)**

B. trigger(continuous="once")

C. processingTime("once")

D. trigger(processingTime="once")

E. processingTime(1)

## Q98

A data engineering team is in the process of converting their existing data pipeline to utilize Auto Loader for incremental processing in the ingestion of JSON files. One data engineer comes across the following code block in the Auto Loader documentation:

```python
(streaming_df = spark.readStream.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", schemaLocation)
.load(sourcePath))
```

Assuming that schemaLocation and sourcePath have been set correctly, which of the following changes does the data engineer need to make to convert this code block to use Auto Loader to ingest the data?

A. The data engineer needs to change the format("cloudFiles") line to format("autoLoader").

B. There is no change required. Databricks automatically uses Auto Loader for streaming reads.

**C. There is no change required. The inclusion of format("cloudFiles") enables the use of Auto Loader.**

D. The data engineer needs to add the .autoLoader line before the .load(sourcePath) line.

E. There is no change required. The data engineer needs to ask their administrator to turn on Auto Loader.

## Q99 - Which of the following data workloads will utilize a Bronze table as its source?

A. A job that aggregates cleaned data to create standard summary statistics

B. A job that queries aggregated data to publish key insights into a dashboard

C. A job that ingests raw data from a streaming source into the Lakehouse

D. A job that develops a feature set for a machine learning application

**E. A job that enriches data by parsing its timestamps into a human-readable format**

## Q100 - Which of the following data workloads will utilize a Silver table as its source?

A. A job that enriches data by parsing its timestamps into a human-readable format

B. A job that queries aggregated data that already feeds into a dashboard

C. A job that ingests raw data from a streaming source into the Lakehouse

**D. A job that aggregates cleaned data to create standard summary statistics**

E. A job that cleans data by removing malformatted records

## Q101 - Which of the following Structured Streaming queries is performing a hop from a Bronze table to a Silver table?

A.
```python
(spark.table("sales")
.groupBy("store")
.agg(sum("sales"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("complete")
.table("aggregatedSales")
)
```

B.
```python
(spark.table("sales")
.agg(sum("sales"),
sum("units"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("complete")
.table("aggregatedSales")
)
```

**C.**
```python
(spark.table("sales")
.withColumn("avgPrice", col("sales") / col("units"))
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("append")
.table("cleanedSales")
)
```

D. 
```python
(spark.readStream.load(rawSalesLocation)
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("append")
.table("uncleanedSales")
)
```

E.
```python
(spark.read.load(rawSalesLocation)
.writeStream
.option("checkpointLocation", checkpointPath)
.outputMode("append")
.table("uncleanedSales")
)
```

## Q102 - Which of the following benefits does Delta Live Tables provide for ELT pipelines over standard data pipelines that utilize Spark and Delta Lake on Databricks?

**A. The ability to declare and maintain data table dependencies**

B. The ability to write pipelines in Python and/or SQL

C. The ability to access previous versions of data tables

D. The ability to automatically scale compute resources

E. The ability to perform batch and streaming queries

## Q103 -  A data engineer has three notebooks in an ELT pipeline. The notebooks need to be executed in a specific order for the pipeline to complete successfully. The data engineer would like to use Delta Live Tables to manage this process. Which of the following steps must the data engineer take as part of implementing this pipeline using Delta Live Tables?

A. They need to create a Delta Live Tables pipeline from the Data page.

**B. They need to create a Delta Live Tables pipeline from the Jobs page.**

C. They need to create a Delta Live tables pipeline from the Compute page.

D. They need to refactor their notebook to use Python and the dlt library.

E. They need to refactor their notebook to use SQL and CREATE LIVE TABLE keyword.

## Q104

A data engineer has written the following query:

```sql
SELECT *
FROM json.`/path/to/json/file.json`;
```

The data engineer asks a colleague for help to convert this query for use in a Delta Live Tables (DLT) pipeline. The query should create the first table in the DLT pipeline.

Which of the following describes the change the colleague needs to make to the query?

A. They need to add a COMMENT line at the beginning of the query.

**B. They need to add a CREATE LIVE TABLE table_name AS line at the beginning of the query.**

C. They need to add a live. prefix prior to json. in the FROM line.

D. They need to add a CREATE DELTA LIVE TABLE table_name AS line at the beginning of the query.

E. They need to add the cloud_files(...) wrapper to the JSON file path.

## Q105

A dataset has been defined using Delta Live Tables and includes an expectations clause:

`CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01')`

What is the expected behavior when a batch of data containing data that violates these constraints is processed?

**A. Records that violate the expectation are added to the target dataset and recorded as invalid in the event log.**

B. Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.

C. Records that violate the expectation cause the job to fail.

D. Records that violate the expectation are added to the target dataset and flagged as invalid in a field added to the target dataset.

E. Records that violate the expectation are dropped from the target dataset and loaded into a quarantine table.

## Q106

A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are defined against Delta Lake table sources using LIVE TABLE. 

The table is configured to run in Development mode using the Triggered Pipeline Mode.

Assuming previously unprocessed data exists and all definitions are valid, what is the expected outcome after clicking Start to update the pipeline?

A. All datasets will be updated once and the pipeline will shut down. The compute resources will be terminated.

B. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will be deployed for the update and terminated when the pipeline is stopped.

C. All datasets will be updated at set intervals until the pipeline is shut down. The compute resources will persist after the pipeline is stopped to allow for additional testing.

**D. All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing.**

E. All datasets will be updated continuously and the pipeline will not shut down. The compute resources will persist with the pipeline.

## Q107 - A data engineer has a Job with multiple tasks that runs nightly. One of the tasks unexpectedly fails during 10 percent of the runs. Which of the following actions can the data engineer perform to ensure the Job completes each night while minimizing compute costs?

A. They can institute a retry policy for the entire Job

B. They can observe the task as it runs to try and determine why it is failing

C. They can set up the Job to run multiple times ensuring that at least one will complete

**D. They can institute a retry policy for the task that periodically fails**

E. They can utilize a Jobs cluster for each of the tasks in the Job

## Q108 - A data engineer has set up two Jobs that each run nightly. The first Job starts at 12:00 AM, and it usually completes in about 20 minutes. The second Job depends on the first Job, and it starts at 12:30 AM. Sometimes, the second Job fails when the first Job does not complete by 12:30 AM. Which of the following approaches can the data engineer use to avoid this problem?

**A. They can utilize multiple tasks in a single job with a linear dependency**

B. They can use cluster pools to help the Jobs run more efficiently

C. They can set up a retry policy on the first Job to help it run more quickly

D. They can limit the size of the output in the second Job so that it will not fail as easily

E. They can set up the data to stream from the first Job to the second Job

## Q109 - A data engineering team has been using a Databricks SQL query to monitor the performance of an ELT job. The ELT job is triggered by a specific number of input records being ready to process. The Databricks SQL query returns the number of minutes since the job’s most recent runtime. Which of the following approaches can enable the data engineering team to be notified if the ELT job has not been run in an hour?

A. They can set up an Alert for the accompanying dashboard to notify them if the returned value is greater than 60.

B. They can set up an Alert for the query to notify when the ELT job fails.

C. They can set up an Alert for the accompanying dashboard to notify when it has not refreshed in 60 minutes.

**D. They can set up an Alert for the query to notify them if the returned value is greater than 60.**

E. This type of alerting is not possible in Databricks.

## Q110 - A data engineering manager has noticed that each of the queries in a Databricks SQL dashboard takes a few minutes to update when they manually click the “Refresh” button. They are curious why this might be occurring, so a team member provides a variety of reasons on why the delay might be occurring. Which of the following reasons *fails* to explain why the dashboard might be taking a few minutes to update?

A. The SQL endpoint being used by each of the queries might need a few minutes to start up.
B. The queries attached to the dashboard might take a few minutes to run under normal circumstances.
C. The queries attached to the dashboard might first be checking to determine if new data is available.
**D. The Job associated with updating the dashboard might be using a non-pooled endpoint.**
E. The queries attached to the dashboard might all be connected to their own, unstarted Databricks clusters.

## Q111 - A new data engineer has started at a company. The data engineer has recently been added to the company’s Databricks workspace as new.engineer@company.com. The data engineer needs to be able to query the table sales in the database retail. The new data engineer already has been granted USAGE on the database retail. Which of the following commands can be used to grant the appropriate permissions to the new data engineer?

A. GRANT USAGE ON TABLE sales TO new.engineer@company.com;

B. GRANT CREATE ON TABLE sales TO new.engineer@company.com;

**C. GRANT SELECT ON TABLE sales TO new.engineer@company.com;**

D. GRANT USAGE ON TABLE new.engineer@company.com TO sales;

E. GRANT SELECT ON TABLE new.engineer@company.com TO sales;

## Q112 - A new data engineer new.engineer@company.com has been assigned to an ELT project. The new data engineer will need full privileges on the table sales to fully manage the project. Which of the following commands can be used to grant full permissions on the table to the new data engineer?

**A. GRANT ALL PRIVILEGES ON TABLE sales TO new.engineer@company.com;**

B. GRANT USAGE ON TABLE sales TO new.engineer@company.com;

C. GRANT ALL PRIVILEGES ON TABLE new.engineer@company.com TO sales;

D. GRANT SELECT ON TABLE sales TO new.engineer@company.com;

E. GRANT SELECT CREATE MODIFY ON TABLE sales TO new.engineer@company.com;
