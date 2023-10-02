A collection of exam question examples of Databricks Certified Associate Developer for Apache Spark 3.0 - Python is available here. Happy learning!

## Q1 - Which of the following describes the Spark driver?

A. The Spark driver is responsible for performing all execution in all execution modes – it is the entire Spark application.

B. The Spare driver is fault tolerant – if it fails, it will recover the entire Spark application.

C. The Spark driver is the coarsest level of the Spark execution hierarchy – it is synonymous with the Spark application.

**D. The Spark driver is the program space in which the Spark application’s main method runs coordinating the Spark entire application.**

E. The Spark driver is horizontally scaled to increase overall processing throughput of a Spark application.

## Q2 - Which of the following describes the relationship between nodes and executors?

A. Executors and nodes are not related.

B. Anode is a processing engine running on an executor.

**C. An executor is a processing engine running on a node.**

D. There are always the same number of executors and nodes.

E. There are always more nodes than executors.

*Explanation:* In Apache Spark, a node refers to a physical or virtual machine in a cluster that is part of the Spark cluster. Each node can have one or more executors running on it (In Databricks it's 1=1). An executor is a Spark component responsible for executing tasks and storing data in memory or on disk. It is a worker process that runs on a node and performs the actual computation and data processing tasks assigned to it by the driver program. Executors are created and managed by the cluster manager, and they are responsible for executing the tasks and managing the data partitions assigned to them.

## Q3 - Which of the following will occur if there are more slots than there are tasks?

**A. The Spark job will likely not run as efficiently as possible.**

B. The Spark application will fail – there must be at least as many tasks as there are slots.

C. Some executors will shut down and allocate all slots on larger executors first.

D. More tasks will be automatically generated to ensure all slots are being used.

E. The Spark job will use just one single slot to perform all tasks.

## Q4 - Which of the following is the most granular level of the Spark execution hierarchy?

**A. Task**

B. Executor

C. Node

D. Job

E. Slot

## Q5 - Which of the following statements about Spark jobs is incorrect?

A. Jobs are broken down into stages.

B. There are multiple tasks within a single job when a DataFrame has more than one partition.

C. Jobs are collections of tasks that are divided up based on when an action is called.

**D. There is no way to monitor the progress of a job.**

E. Jobs are collections of tasks that are divided based on when language variables are defined.

*Explanation:* Spark provides various tools and interfaces for monitoring the progress of a job, including the Spark UI, which provides real-time information about the job's stages, tasks, and resource utilization. Other tools, such as the Spark History Server, can be used to view completed job information.

## Q6 - Which of the following operations is most likely to result in a shuffle?

**A. DataFrame.join()**

B. DataFrame.filter()

C. DataFrame.union()

D. DataFrame.where()

E. DataFrame.drop()

*Explanation:* A shuffle operation in Spark involves redistributing and reorganizing data across partitions. It typically occurs when data needs to be rearranged or merged based on a specific key or condition. DataFrame joins involve combining two DataFrames based on a common key column, and this operation often requires data to be shuffled to ensure that matching records are located on the same executor or partition. The shuffle process involves exchanging data between nodes or executors in the cluster, which can incur significant data movement and network communication overhead.

## Q7 - The default value of spark.sql.shuffle.partitions is 200. Which of the following describes what that means?

A. By default, all DataFrames in Spark will be spit to perfectly fill the memory of 200 executors.

B. By default, new DataFrames created by Spark will be split to perfectly fill the memory of 200 executors.

C. By default, Spark will only read the first 200 partitions of DataFrames to improve speed.

D. By default, all DataFrames in Spark, including existing DataFrames, will be split into 200 unique segments for parallelization.

**E. By default, DataFrames will be split into 200 unique partitions when data is being shuffled.**

*Explanation:* When a shuffle operation occurs, such as during DataFrame joins or aggregations, data needs to be redistributed across partitions based on a specific key. The `spark.sql.shuffle.partitions` value defines the default number of partitions to be used during such shuffling operations.

## Q8 - Which of the following is the most complete description of lazy evaluation?

A. None of these options describe lazy evaluation

**B. A process is lazily evaluated if its execution does not start until it is put into action by some type of trigger**

C. A process is lazily evaluated if its execution does not start until it is forced to display a result to the user

D. A process is lazily evaluated if its execution does not start until it reaches a specified date and time

E. A process is lazily evaluated if its execution does not start until it is finished compiling

*Explanation:* Lazy evaluation is a programming language feature that delays the evaluation of expressions or computations until the result is actually needed or requested. In a lazily evaluated system, expressions are not immediately executed or evaluated when they are defined or assigned. Instead, the evaluation is deferred until the value is needed by another part of the program or when an action is triggered.

## Q9 - Which of the following DataFrame operations is classified as an action?

A. DataFrame.drop()

B. DataFrame.coalesce()

**C. DataFrame.take()**

D. DataFrame.join()

E. DataFrame.filter()

*Explanation:* In Spark, actions are operations that trigger the execution of transformations on a DataFrame and return results or side effects. Actions are evaluated eagerly, meaning they initiate the execution of the computation plan built by transformations. Among the options provided, DataFrame.take() is an action because it returns an array with the first n elements from the DataFrame as an array. It triggers the execution of any pending transformations and collects the resulting data.

## Q10 - Which of the following DataFrame operations is classified as a wide transformation?

A. DataFrame.filter()

**B. DataFrame.join()**

C. DataFrame.select()

D. DataFrame.drop()

E. DataFrame.union()

*Explanation:* In Spark, transformations are operations on DataFrames that create a new DataFrame from an existing one. Wide transformations involve shuffling or redistributing data across partitions and typically require data movement across the network. Among the options provided, DataFrame.join() is a wide transformation because it involves combining two DataFrames based on a common key column, which often requires shuffling and redistributing the data across partitions.

## Q11 - Which of the following describes the difference between cluster and client execution modes?

**A. The cluster execution mode runs the driver on a worker node within a cluster, while the client execution mode runs the driver on the client machine (also known as a gateway machine or edge node).**

B. The cluster execution mode is run on a local cluster, while the client execution mode is run in the cloud.

C. The cluster execution mode distributes executors across worker nodes in a cluster, while the client execution mode runs a Spark job entirely on one client machine.

D. The cluster execution mode runs the driver on the cluster machine (also known as a gateway machine or edge node), while the client execution mode runs the driver on a worker node within a cluster.

E. The cluster execution mode distributes executors across worker nodes in a cluster, while the client execution mode submits a Spark job from a remote machine to be run on a remote, unconfigurable cluster.

## Q12 - Which of the following statements about Spark’s stability is incorrect?

A. Spark is designed to support the loss of any set of worker nodes.

B. Spark will rerun any failed tasks due to failed worker nodes.

C. Spark will recompute data cached on failed worker nodes.

D. Spark will spill data to disk if it does not fit in memory.

**E. Spark will reassign the driver to a worker node if the driver’s node fails.**

*Explanation:* The driver program is responsible for the coordination and control of the Spark application and runs on a separate machine, typically the client machine or cluster manager. If the driver's node fails, the Spark application as a whole may fail or need to be restarted, but the driver is not automatically reassigned to another worker node.

## Q13 - Of the following situations, in which will it be most advantageous to store DataFrame df at the MEMORY_AND_DISK storage level rather than the MEMORY_ONLY storage level?

A. When all of the computed data in DataFrame df can fit into memory.

B. When the memory is full and it’s faster to recompute all the data in DataFrame df rather than read it from disk.

C. When it’s faster to recompute all the data in DataFrame df that cannot fit into memory based on its logical plan rather than read it from disk.

**D. When it’s faster to read all the computed data in DataFrame df that cannot fit into memory from disk rather than recompute it based on its logical plan.**

E. The storage level MENORY_ONLY will always be more advantageous because it’s faster to read data from memory than it is to read data from disk.

*Explanation:* MEMORY_AND_DISK, which is the default mode for cache or persist. That means, if the data size is larger than the memory, it will store the extra data in disk. next time when we need to read data, we will read data firstly from memory, and then read from disk. On the other hand, MEMORY_ONLY means, if the data size is larger than memory, it will not store the extra data. next time we read data, we will read from memory first and then recompute the extra data which cannot store in memory. Therefore, the difference/balance between MEMORY_ONLY and MEMORY_AND_DISK lay in how they handle the extra data out of memory. which is option D, if it is faster to read data from disk is faster than recompute it, then MEMORY_AND_DISK.

## Q14 - A Spark application has a 128 GB DataFrame A and a 1 GB DataFrame B. If a broadcast join were to be performed on these two DataFrames, which of the following describes which DataFrame should be broadcasted and why?

A. Either DataFrame can be broadcasted. Their results will be identical in result and efficiency.

B. DataFrame B should be broadcasted because it is smaller and will eliminate the need for the shuffling of itself.

C. DataFrame A should be broadcasted because it is larger and will eliminate the need for the shuffling of DataFrame B.

**D. DataFrame B should be broadcasted because it is smaller and will eliminate the need for the shuffling of DataFrame A.**

E. DataFrame A should be broadcasted because it is smaller and will eliminate the need for the shuffling of itself.

## Q15 - Which of the following operations can be used to create a new DataFrame that has 12 partitions from an original DataFrame df that has 8 partitions?

**A. df.repartition(12)**

B. df.cache()

C. df.partitionBy(1.5)

D. df.coalesce(12)

E. df.partitionBy(12)

*Explanation:* The `coalesce()` operation in Spark is used to decrease the number of partitions in a DataFrame, and it can be used to create a new DataFrame with a specific number of partitions. In this case, calling df.coalesce(12) on the original DataFrame df with 8 partitions will create a new DataFrame with 12 partitions. So right approach would be with `repartition()`

## Q16 - Which of the following Spark properties is used to configure the maximum size of an automatically broadcasted DataFrame when performing a join?

A. spark.sql.broadcastTimeout

**B. spark.sql.autoBroadcastJoinThreshold**

C. spark.sql.shuffle.partitions

D. spark.sql.inMemoryColumnarStorage.batchSize

E. spark.sql.adaptive.skewedJoin.enabled

## Q17 - Which of the following object types cannot be contained within a column of a Spark DataFrame?

**A. DataFrame**

B. String

C. Array

D. null

E. Vector

## Q18 - Which of the following operations can be used to create a DataFrame with a subset of columns from DataFrame storesDF that are specified by name?

A. storesDF.subset()

**B. storesDF.select()**

C. storesDF.selectColumn()

D. storesDF.filter()

E. storesDF.drop()

## Q19 - The code block shown below contains an error. The code block is intended to return a DataFrame containing all columns from DataFrame storesDF except for column sqft and column customerSatisfaction. Identify the error.

*Code block:* `storesDF.drop(sqft, customerSatisfaction)`

A. The drop() operation only works if one column name is called at a time – there should be two calls in succession like storesDF.drop("sqft").drop("customerSatisfaction").

B. The drop() operation only works if column names are wrapped inside the col() function like storesDF.drop(col(sqft), col(customerSatisfaction)).

C. There is no drop() operation for storesDF.

**D. The sqft and customerSatisfaction column names should be quoted like "sqft" and "customerSatisfaction".**

E. The sqft and customerSatisfaction column names should be subset from the DataFrame storesDF like storesDF."sqft" and storesDF."customerSatisfaction".

## Q20 - Which of the following code blocks returns a DataFrame containing only the rows from DataFrame storesDF where the value in column sqft is less than or equal to 25,000?

A. storesDF.filter("sqft" <= 25000)

B. storesDF.filter(sqft > 25000)

C. storesDF.where(storesDF[sqft] > 25000)

D. storesDF.where(sqft > 25000)

**E. storesDF.filter(col("sqft") <= 25000)**

## Q21 - Which of the following code blocks returns a DataFrame containing only the rows from DataFrame storesDF where the value in column sqft is less than or equal to 25,000 OR the value in column customerSatisfaction is greater than or equal to 30?

A. storesDF.filter(col("sqft") <= 25000 | col("customerSatisfaction") >= 30)

B. storesDF.filter(col("sqft") <= 25000 or col("customerSatisfaction") >= 30)

C. storesDF.filter(sqft <= 25000 or customerSatisfaction >= 30)

D. storesDF.filter(col(sqft) <= 25000 | col(customerSatisfaction) >= 30)

**E. storesDF.filter((col("sqft") <= 25000) | (col("customerSatisfaction") >= 30))**

## Q22 - Which of the following code blocks returns a new DataFrame from DataFrame storesDF where column storeId is of the type string?

A. storesDF.withColumn("storeId", cast(col("storeId"), StringType()))

**B. storesDF.withColumn("storeId", col("storeId").cast(StringType()))**

C. storesDF.withColumn("storeId", cast(storeId).as(StringType)

D. storesDF.withColumn("storeId", col(storeId).cast(StringType)

E. storesDF.withColumn("storeId", cast("storeId").as(StringType()))

## Q23 - Which of the following code blocks returns a new DataFrame with a new column employeesPerSqft that is the quotient of column numberOfEmployees and column sqft, both of which are from DataFrame storesDF? Note that column employeesPerSqft is not in the original DataFrame storesDF.

**A. storesDF.withColumn("employeesPerSqft", col("numberOfEmployees") / col("sqft"))**

B. storesDF.withColumn("employeesPerSqft", "numberOfEmployees" / "sqft")

C. storesDF.select("employeesPerSqft", "numberOfEmployees" / "sqft")

D. storesDF.select("employeesPerSqft", col("numberOfEmployees") / col("sqft"))

E. storesDF.withColumn(col("employeesPerSqft"), col("numberOfEmployees") / col("sqft"))

## Q24 - The code block shown below should return a new DataFrame from DataFrame storesDF where column modality is the constant string "PHYSICAL", Assume DataFrame storesDF is the only defined language variable. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.

*Code block:* `storesDF. _1_(_2_,_3_(_4_))`

A.  1. withColumn
    2. "modality"
    3. col
    4. "PHYSICAL"
    
B.  1. withColumn
    2. "modality"
    3. lit
    4. PHYSICAL
    
<b> C.  1. withColumn
    2. "modality"
    3. lit
    4. "PHYSICAL" </b>
    
D.  1. withColumn
    2. "modality"
    3. SrtringType
    4. "PHYSICAL"
    
E.  1. newColumn
    2. modality
    3. SrtringType
    4. PHYSICAL

## Q25 - Which of the following code blocks returns a DataFrame where column storeCategory from DataFrame storesDF is split at the underscore character into column storeValueCategory and column storeSizeCategory?

A. (storesDF.withColumn("storeValueCategory", split(col("storeCategory"), "_")[1]) \

  .withColumn("storeSizeCategory", split(col("storeCategory"), "_")[2]))
  
B. (storesDF.withColumn("storeValueCategory", col("storeCategory").split("_")[0]) \

  .withColumn("storeSizeCategory", col("storeCategory").split("_")[1]))
  
<b> C. (storesDF.withColumn("storeValueCategory", split(col("storeCategory"), "_")[0]) \

  .withColumn("storeSizeCategory", split(col("storeCategory"), "_")[1])) </b>

D. (storesDF.withColumn("storeValueCategory", split("storeCategory", "_")[0]) \

.withColumn("storeSizeCategory", split("storeCategory", "_")[1]))

E. (storesDF.withColumn("storeValueCategory", col("storeCategory").split("_")[1]) \

.withColumn("storeSizeCategory", col("storeCategory").split("_")[2]))

## Q26 - Which of the following code blocks returns a new DataFrame where column productCategories only has one word per row, resulting in a DataFrame with many more rows than DataFrame storesDF?

A sample of storesDF is displayed below:

```python
storeID, productCategories
0, [ABC, DEF, ..]
1, [DEF, XYZ, ..]
2, [XYZ]
```

**A. storesDF.withColumn("productCategories", explode(col("productCategories")))**

B. storesDF.withColumn("productCategories", split(col("productCategories")))

C. storesDF.withColumn("productCategories", col("productCategories").explode())

D. storesDF.withColumn("productCategories", col("productCategories").split())

E. storesDF.withColumn("productCategories", explode("productCategories"))

## Q27 - Which of the following code blocks returns a new DataFrame with column storeDescription where the pattern "Description: " has been removed from the beginning of column storeDescription in DataFrame storesDF?

A. storesDF.withColumn("storeDescription", regexp_replace(col("storeDescription"), "^Description: "))

B. storesDF.withColumn("storeDescription", col("storeDescription").regexp_replace("^Description: ", ""))

C. storesDF.withColumn("storeDescription", regexp_extract(col("storeDescription"), "^Description: ", ""))

D. storesDF.withColumn("storeDescription", regexp_replace("storeDescription", "^Description: ", ""))

**E. storesDF.withColumn("storeDescription", regexp_replace(col("storeDescription"), "^Description: ", ""))**

## Q28 - Which of the following code blocks returns a new DataFrame where column division from DataFrame storesDF has been replaced and renamed to column state and column managerName from DataFrame storesDF has been replaced and renamed to column managerFullName?

A. (storesDF.withColumnRenamed(["division", "state"], ["managerName", "managerFullName"])

B. (storesDF.withColumn("state", col("division")) \

  .withColumn("managerFullName", col("managerName")))

C. (storesDF.withColumn("state", "division") \

  .withColumn("managerFullName", "managerName"))

D. (storesDF.withColumnRenamed("state", "division") \

  .withColumnRenamed("managerFullName", "managerName"))

<b> E. (storesDF.withColumnRenamed("division", "state") \

  .withColumnRenamed("managerName", "managerFullName")) </b>

## Q29 - The code block shown contains an error. The code block is intended to return a new DataFrame where column sqft from DataFrame storesDF has had its missing values replaced with the value 30,000. Identify the error.

*Code block:* `storesDF.na.fill(30000, col("sqft"))`

**A. The argument to the subset parameter of fill() should be a string column name or a list of string column names rather than a Column object.**

B. The na.fill() operation does not work and should be replaced by the dropna() operation.

C. he argument to the subset parameter of fill() should be a the numerical position of the column rather than a Column object.

D. The na.fill() operation does not work and should be replaced by the nafill() operation.

E. The na.fill() operation does not work and should be replaced by the fillna() operation.

## Q30 - Which of the following operations fails to return a DataFrame with no duplicate rows?

A. DataFrame.dropDuplicates()

B. DataFrame.distinct()

C. DataFrame.drop_duplicates()

D. DataFrame.drop_duplicates(subset = None)

**E. DataFrame.drop_duplicates(subset = "all")**

## Q31 - Which of the following code blocks will most quickly return an approximation for the number of distinct values in column division in DataFrame storesDF?

**A. storesDF.agg(approx_count_distinct(col("division")).alias("divisionDistinct"))**

B. storesDF.agg(approx_count_distinct(col("division"), 0.01).alias("divisionDistinct"))

C. storesDF.agg(approx_count_distinct(col("division"), 0.15).alias("divisionDistinct"))

D. storesDF.agg(approx_count_distinct(col("division"), 0.0).alias("divisionDistinct"))

E. storesDF.agg(approx_count_distinct(col("division"), 0.05).alias("divisionDistinct"))

## Q32 - Which of the following Spark properties is used to configure whether skewed partitions are automatically detected and subdivided into smaller partitions when joining two DataFrames together?

A. spark.sql.adaptive.skewedJoin.enabled

**B. spark.sql.adaptive.coalescePartitions.enable**

C. spark.sql.adaptive.skewHints.enabled

D. spark.sql.shuffle.partitions

E. spark.sql.shuffle.skewHints.enabled

## Q33 - Which of the following operations can be used to return the number of rows in a DataFrame?

A. DataFrame.numberOfRows()

B. DataFrame.n()

C. DataFrame.sum()

**D. DataFrame.count()**

E. DataFrame.countDistinct()

## Q34 - Which of the following operations returns a GroupedData object?

A. DataFrame.GroupBy()

B. DataFrame.cubed()

C. DataFrame.group()

**D. DataFrame.groupBy()**

E. DataFrame.grouping_id()

## Q35 - Which of the following code blocks returns a collection of summary statistics for all columns in DataFrame storesDF?

A. storesDF.summary("mean")

B. storesDF.describe(all = True)

C. storesDF.describe("all")

D. storesDF.summary("all")

**E. storesDF.describe()**

## Q36 - Which of the following code blocks fails to return a DataFrame reverse sorted alphabetically based on column division?

A. storesDF.orderBy("division", ascending = False)

B. storesDF.orderBy(["division"], ascending = [0])

**C. storesDF.orderBy(col("division").asc())**

D. storesDF.sort("division", ascending = False)

E. storesDF.sort(desc("division"))

## Q37 - Which of the following code blocks returns a 15 percent sample of rows from DataFrame storesDF without replacement?

A. storesDF.sample(fraction = 0.10)

B. storesDF.sampleBy(fraction = 0.15)

C. storesDF.sample(True, fraction = 0.10)

D. storesDF.sample()

**E. storesDF.sample(fraction = 0.15)**

## Q38 - Which of the following code blocks returns all the rows from DataFrame storesDF?

A. storesDF.head()

**B. storesDF.collect()**

C. storesDF.count()

D. storesDF.take()

E. storesDF.show()

## Q39 - Which of the following code blocks applies the function assessPerformance() to each row of DataFrame storesDF?

A. [assessPerformance(row) for row in storesDF.take(3)]

B. [assessPerformance() for row in storesDF]

C. storesDF.collect().apply(lambda: assessPerformance)

**D. [assessPerformance(row) for row in storesDF.collect()]**

E. [assessPerformance(row) for row in storesDF]

## Q40 - The code block shown below contains an error. The code block is intended to print the schema of DataFrame storesDF. Identify the error.

*Code block:* `storesDF.printSchema`

A. There is no printSchema member of DataFrame – schema and the print() function should be used instead.

B. The entire line needs to be a string – it should be wrapped by str().

C. There is no printSchema member of DataFrame – the getSchema() operation should be used instead.

D. There is no printSchema member of DataFrame – the schema() operation should be used instead.

**E. The printSchema member of DataFrame is an operation and needs to be followed by parentheses.**

## Q41 - The code block shown below should create and register a SQL UDF named "ASSESS_PERFORMANCE" using the Python function assessPerformance() and apply it to column customerSatisfaction in table stores. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.

*Code blocks:*

```python
spark._1_._2_(_3_,_4_)
spark.sql("SELECT customerSatisfaction, _5_(customerSatisfaction) AS result FROM stores")
```

<b> A.  1. udf
    2. register
    3. "ASSESS_PERFORMANCE"
    4. assessPerformance
    5. ASSESS_PERFORMANCE </b>

B.  1. udf
    2. register
    3. assessPerformance
    4. "ASSESS_PERFORMANCE"
    5. "ASSESS_PERFORMANCE"

C.  1. udf
    2. register
    3."ASSESS_PERFORMANCE"
    4. assessPerformance
    5. "ASSESS_PERFORMANCE"

D.  1. register
    2. udf
    3. "ASSESS_PERFORMANCE"
    4. assessPerformance
    5. "ASSESS_PERFORMANCE"

E.  1. udf
    2. register
    3. ASSESS_PERFORMANCE
    4. assessPerformance
    5. ASSESS_PERFORMANCE

## Q42 - The code block shown below contains an error. The code block is intended to create a Python UDF assessPerformanceUDF() using the integer-returning Python function assessPerformance() and apply it to column customerSatisfaction in DataFrame storesDF. Identify the error.

*Code block:*

```python
assessPerformanceUDF = udf(assessPerformance)
storesDF.withColumn("result", assessPerformanceUDF(col("customerSatisfaction")))
```

A. The assessPerformance() operation is not properly registered as a UDF.

B. The withColumn() operation is not appropriate here – UDFs should be applied by iterating over rows instead.

C. UDFs can only be applied vie SQL and not through the DataFrame API.

**D. The return type of the assessPerformanceUDF() is not specified in the udf() operation.**

E. The assessPerformance() operation should be used on column customerSatisfaction rather than the assessPerformanceUDF() operation.

Explanation: `pyspark.sql.functions.udf(f=None, returnType=StringType)` --> the default return type is string, but this question requires integer returning.

## Q43 - The code block shown below contains an error. The code block is intended to use SQL to return a new DataFrame containing column storeId and column managerName from a table created from DataFrame storesDF. Identify the error.

*Code block:*

```python
storesDF.createOrReplaceTempView("stores")
storesDF.sql("SELECT storeId, managerName FROM stores")
```

A. The createOrReplaceTempView() operation does not make a Dataframe accessible via SQL.

**B. The sql() operation should be accessed via the spark variable rather than DataFrame storesDF.**

C. There is the sql() operation in DataFrame storesDF. The operation query() should be used instead.

D. This cannot be accomplished using SQL – the DataFrame API should be used instead.

E. The createOrReplaceTempView() operation should be accessed via the spark variable rather than DataFrame storesDF.

## Q44 - The code block shown below should create a single-column DataFrame from Python list years which is made up of integers. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.

*Code block:* `_1_._2_(_3_,_4_)`

A.  1. spark
    2. createDataFrame
    3. years
    4. IntegerType

B.  1. DataFrame
    2. create
    3. [years]
    4. IntegerType

C.  1. spark
    2. createDataFrame
    3. [years]
    4. IntegertType

D.  1. spark
    2. createDataFrame
    3. [years]
    4. IntegertType()

<b> E.  1. spark
    2. createDataFrame
    3. years
    4. IntegertType() </b>

## Q45 - The code block shown below contains an error. The code block is intended to cache DataFrame storesDF only in Spark’s memory and then return the number of rows in the cached DataFrame. Identify the error.

*Code block:* `storesDF.cache().count()`

A. The cache() operation caches DataFrames at the MEMORY_AND_DISK level by default – the storage level must be specified to MEMORY_ONLY as an argument to cache().

B. The cache() operation caches DataFrames at the MEMORY_AND_DISK level by default – the storage level must be set via storesDF.storageLevel prior to calling cache().

C. The storesDF DataFrame has not been checkpointed – it must have a checkpoint in order to be cached.

D. DataFrames themselves cannot be cached – DataFrame storesDF must be cached as a table.

**E. The cache() operation can only cache DataFrames at the MEMORY_AND_DISK level (the default) – persist() should be used instead.**

## Q46 - Which of the following operations can be used to return a new DataFrame from DataFrame storesDF without inducing a shuffle?

A. storesDF.intersect()

B. storesDF.repartition(1)

C. storesDF.union()

**D. storesDF.coalesce(1)**

E. storesDF.rdd.getNumPartitions()

## Q47 - The code block shown below contains an error. The code block is intended to return a new 12-partition DataFrame from the 8-partition DataFrame storesDF by inducing a shuffle. Identify the error.

*Code block:* `storesDF.coalesce(12)`

A. The coalesce() operation cannot guarantee the number of target partitions – the repartition() operation should be used instead.

**B. The coalesce() operation does not induce a shuffle and cannot increase the number of partitions – the repartition() operation should be used instead.**

C. The coalesce() operation will only work if the DataFrame has been cached to memory – the repartition() operation should be used instead.

D. The coalesce() operation requires a column by which to partition rather than a number of partitions – the repartition() operation should be used instead.

E. The number of resulting partitions, 12, is not achievable for an 8-partition DataFrame.

## Q48 - Which of the following Spark properties is used to configure whether DataFrame partitions that do not meet a minimum size threshold are automatically coalesced into larger partitions during a shuffle?

A. spark.sql.shuffle.partitions

B. spark.sql.autoBroadcastJoinThreshold

C. spark.sql.adaptive.skewJoin.enabled

D. spark.sql.inMemoryColumnarStorage.batchSize

**E. spark.sql.adaptive.coalescePartitions.enabled**

## Q49 - The code block shown below contains an error. The code block is intended to return a DataFrame containing a column openDateString, a string representation of Java’s SimpleDateFormat. Identify the error.

Note that column openDate is of type integer and represents a date in the UNIX epoch format – the number of seconds since midnight on January 1st, 1970. An example of Java’s SimpleDateFormat is "Sunday, Dec 4, 2008 1:05 PM".

*Code block:* `storesDF.withColumn("openDateString", from_unixtime(col("openDate"), "EEE, MMM d, yyyy h:mm a", TimestampType()))`

**A. The from_unixtime() operation only accepts two parameters – the TimestampTime() arguments not necessary.**

B. The from_unixtime() operation only works if column openDate is of type long rather than integer – column openDate must first be converted.

C. The second argument to from_unixtime() is not correct – it should be a variant of TimestampType() rather than a string.

D. The from_unixtime() operation automatically places the input column in java’s SimpleDateFormat – there is no need for a second or third argument.

E. The column openDate must first be converted to a timestamp, and then the Date() function can be used to reformat to java’s SimpleDateFormat.

## Q50 - Which of the following code blocks returns a DataFrame containing a column dayOfYear, an integer representation of the day of the year from column openDate from DataFrame storesDF?

Note that column openDate is of type integer and represents a date in the UNIX epoch format – the number of seconds since midnight on January 1st, 1970.

<b> A. (storesDF.withColumn("openTimestamp", col("openDate").cast("Timestamp")) \

  .withColumn("dayOfYear", dayofyear(col("openTimestamp")))) </b>

B. storesDF.withColumn("dayOfYear", get dayofyear(col("openDate")))

C. storesDF.withColumn("dayOfYear", dayofyear(col("openDate")))

D. (storesDF.withColumn("openDateFormat", col("openDate").cast("Date"))

  .withColumn("dayOfYear", dayofyear(col("openDateFormat"))))

E. storesDF.withColumn("dayOfYear", substr(col("openDate"), 4, 6))

*Explanation:* `dayofyear` function in PySpark's functions module expects the column openDate to be of type timestamp rather than long.

## Q51 - The code block shown below contains an error. The code block intended to return a new DataFrame that is the result of an inner join between DataFrame storesDF and DataFrame employeesDF on column storeId. Identify the error.

*Code block:* `StoresDF.join(employeesDF, "inner", "storeID")`

A. The key column storeID needs to be wrapped in the col() operation.

B. The key column storeID needs to be in a list like ["storeID"].

C. The key column storeID needs to be specified in an expression of both DataFrame columns like storesDF.storeId == employeesDF.storeId.

D. There is no DataFrame.join() operation – DataFrame.merge() should be used instead.

**E. The column key is the second parameter to join() and the type of join in the third parameter to join() – the second and third arguments should be switched.**

## Q52 - Which of the following operations can perform an outer join on two DataFrames?

A. DataFrame.crossJoin()

B. Standalone join() function

C. DataFrame.outerJoin()

**D. DataFrame.join()**

E. DataFrame.merge()

*Explanation:* `result_df = df1.join(df2, on="key", how="outer")`

## Q53 - Which of the following pairs of arguments cannot be used in DataFrame.join() to perform an inner join on two DataFrames, named and aliased with "a" and "b" respectively, to specify two key columns?

A. on = [a.column1 == b.column1, a.column2 == b.column2]

B. on = [col("column1"), col("column2")]

C. on = [col("a.column1") == col("b.column1"), col("a.column2") == col("b.column2")]

**D. All of these options can be used to perform an inner join with two key columns.**

E. on = ["column1", "column2"]

## Q54 - The below code block contains a logical error resulting in inefficiency. The code block is intended to efficiently perform a broadcast join of DataFrame storesDF and the much larger DataFrame employeesDF using key column storeId. Identify the logical error.

*Code block:* `storesDF.join(broadcast(employeesDF), "storeId")`

**A. The larger DataFrame employeesDF is being broadcasted rather than the smaller DataFrame storesDF.**

B. There is never a need to call the broadcast() operation in Apache Spark 3.

C. The entire line of code should be wrapped in broadcast() rather than just DataFrame employeesDF.

D. The broadcast() operation will only perform a broadcast join if the Spark property spark.sql.autoBroadcastJoinThreshold is manually set.

E. Only one of the DataFrames is being broadcasted rather than both of the DataFrames.

## Q55 - The code block shown below contains an error. The code block is intended to return a new DataFrame that is the result of a cross join between DataFrame storesDF and DataFrame employeesDF. Identify the error.

*Code block:* `storesDF.join(employeesDF, "cross")`

A. A cross join is not implemented by the DataFrame.join() operations – the standalone CrossJoin() operation should be used instead.

B. There is no direct cross join in Spark, but it can be implemented by performing an outer join on all columns of both DataFrames.

**C. A cross join is not implemented by the DataFrame.join()operation – the DataFrame.crossJoin()operation should be used instead.**

D. There is no key column specified – the key column "storeId" should be the second argument.

E. A cross join is not implemented by the DataFrame.join() operations – the standalone join() operation should be used instead.

*Explanation:* Use either `result_df = df1.join(df2, on="key", how="cross")` or `result_df = df1.crossJoin(df2.select(".."))`

## Q56 - The code block shown below contains an error. The code block is intended to return a new DataFrame that is the result of a position-wise union between DataFrame storesDF and DataFrame acquiredStoresDF. Identify the error.

*Code block:* `storesDF.unionByName(acquiredStoresDF)`

A. There is no DataFrame.unionByName() operation – the concat() operation should be used instead with both DataFrames as arguments.

B. There are no key columns specified – similar column names should be the second argument.

**C. The DataFrame.unionByName() operation does not union DataFrames based on column position – it uses column name instead.**

D. The unionByName() operation is a standalone operation rather than a method of DataFrame – it should have both DataFrames as arguments.

E. There are no column positions specified – the desired column positions should be the second argument.

*Explanation:* See [pyspark.sql.DataFrame.unionByName](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html)

## Q57 - Which of the following code blocks writes DataFrame storesDF to file path filePath as JSON?

A. storesDF.write.option("json").path(filePath)

**B. storesDF.write.json(filePath)**

C. storesDF.write.path(filePath)

D. storesDF.write(filePath)

E. storesDF.write().json(filePath)

## Q58 - In what order should the below lines of code be run in order to write DataFrame storesDF to file path filePath as parquet and partition by values in column division?

Lines of code:
  1. .write()
  2. .partitionBy("division")
  3. .parquet(filePath)
  4. .storesDF
  5. .repartition("division")
  6. .write
  7. .path(filePath, "parquet")

A. 4, 1, 2, 3

B. 4, 1, 5, 7

**C. 4, 6, 2, 3**

D. 4, 1, 5, 3

E. 4, 6, 2, 7

## Q59 - The code block shown below contains an error. The code block intended to read a parquet at the file path filePath into a DataFrame. Identify the error.

*Code block:* `spark.read.load(filePath, source – "parquet")`

A. There is no source parameter to the load() operation – the schema parameter should be used instead.

**B. There is no load() operation – it should be parquet() instead.**

C. The spark.read operation should be followed by parentheses to return a DataFrameReader object.

D. The filePath argument to the load() operation should be quoted.

E. There is no source parameter to the load() operation – it can be removed.

## Q60 - In what order should the below lines of code be run in order to read a JSON file at the file path filePath into a DataFrame with the specified schema schema?

Lines of code:
  1. .json(filePath, schema = schema)
  2. .storesDF
  3. .spark
  4. .read()
  5. .read
  6. .json(filePath, format = schema) </i>

A. 3, 5, 6

B. 2, 4, 1

**C. 3, 5, 1**

D. 2, 5, 1

E. 3, 4, 1

## Q61 - Which of the following statements about the Spark driver is incorrect?

A. The Spark driver is the node in which the Spark application's main method runs to coordinate the Spark application.

**B. The Spark driver is horizontally scaled to increase overall processing throughput.**

C. The Spark driver contains the SparkContext object.

D. The Spark driver is responsible for scheduling the execution of data by various worker nodes in cluster mode.

E. The Spark driver should be as close as possible to worker nodes for optimal performance.

## Q62 - Which of the following describes nodes in cluster-mode Spark?

A. Nodes are the most granular level of execution in the Spark execution hierarchy.

B. There is only one node and it hosts both the driver and executors.

C. Nodes are another term for executors, so they are processing engine instances for performing computations.

D. There are driver nodes and worker nodes, both of which can scale horizontally.

**E. Worker nodes are machines that host the executors responsible for the execution of tasks.**

## Q63 - Which of the following statements about slots is true?

A. There must be more slots than executors.

B. There must be more tasks than slots.

C. Slots are the most granular level of execution in the Spark execution hierarchy.

D. Slots are not used in cluster mode.

**E. Slots are resources for parallelization within a Spark application.**

## Q64 - Which of the following is a combination of a block of data and a set of transformers that will run on a single executor?

A. Executor

B. Node

C. Job

**D. Task**

E. Slot

## Q65 - Which of the following is a group of tasks that can be executed in parallel to compute the same set of operations on potentially multiple machines?

A. Job

B. Slot

C. Executor

D. Task

**E. Stage**

## Q66 - Which of the following describes a shuffle?

**A. A shuffle is the process by which data is compared across partitions.**

B. A shuffle is the process by which data is compared across executors.

C. A shuffle is the process by which partitions are allocated to tasks.

D. A shuffle is the process by which partitions are ordered for write.

E. A shuffle is the process by which tasks are ordered for execution.

## Q67 - DataFrame df is very large with a large number of partitions, more than there are executors in the cluster. Based on this situation, which of the following is incorrect? Assume there is one core per executor.

**A. Performance will be suboptimal because not all executors will be utilized at the same time.**

B. Performance will be suboptimal because not all data can be processed at the same time.

C. There will be a large number of shuffle connections performed on DataFrame df when operations inducing a shuffle are called.

D. There will be a lot of overhead associated with managing resources for data processing within each task.

E. There might be risk of out-of-memory errors depending on the size of the executors in the cluster.

## Q68 - Which of the following operations will trigger evaluation?

A. DataFrame.filter()

B. DataFrame.distinct()

C. DataFrame.intersect()

D. DataFrame.join()

**E. DataFrame.count()**

## Q69 - Which of the following describes the difference between transformations and actions?

A. Transformations work on DataFrames/Datasets while actions are reserved for native language objects.

B. There is no difference between actions and transformations.

C. Actions are business logic operations that do not induce execution while transformations are execution triggers focused on returning results.

D. Actions work on DataFrames/Datasets while transformations are reserved for native language objects.

**E. Transformations are business logic operations that do not induce execution while actions are execution triggers focused on returning results.**

## Q70 - Which of the following DataFrame operations is always classified as a narrow transformation?

A. DataFrame.sort()

B. DataFrame.distinct()

C. DataFrame.repartition()

**D. DataFrame.select()**

E. DataFrame.join()

## Q71 - Spark has a few different execution/deployment modes: cluster, client, and local. Which of the following describes Spark's execution/deployment mode?

**A. Spark's execution/deployment mode determines where the driver and executors are physically located when a Spark application is run**

B. Spark's execution/deployment mode determines which tasks are allocated to which executors in a cluster

C. Spark's execution/deployment mode determines which node in a cluster of nodes is responsible for running the driver program

D. Spark's execution/deployment mode determines exactly how many nodes the driver will connect to when a Spark application is run

E. Spark's execution/deployment mode determines whether results are run interactively in a notebook environment or in batch

## Q72 - Which of the following describes out-of-memory errors in Spark?

**A. An out-of-memory error occurs when either the driver or an executor does not have enough memory to collect or process the data allocated to it.**

B. An out-of-memory error occurs when Spark's storage level is too lenient and allows data objects to be cached to both memory and disk.

C. An out-of-memory error occurs when there are more tasks than are executors regardless of the number of worker nodes.

D. An out-of-memory error occurs when the Spark application calls too many transformations in a row without calling an action regardless of the size of the data object on which the transformations are operating.

E. An out-of-memory error occurs when too much data is allocated to the driver for computational purposes.

## Q73 - Which of the following is the default storage level for persist() for a non-streaming DataFrame/Dataset?

**A. MEMORY_AND_DISK**

B. MEMORY_AND_DISK_SER

C. DISK_ONLY

D. MEMORY_ONLY_SER

E. MEMORY_ONLY

## Q74 - Which of the following describes a broadcast variable?

A. A broadcast variable is a Spark object that needs to be partitioned onto multiple worker nodes because it's too large to fit on a single worker node.

B. A broadcast variable can only be created by an explicit call to the broadcast() operation.

C. A broadcast variable is entirely cached on the driver node so it doesn't need to be present on any worker nodes.

**D. A broadcast variable is entirely cached on each worker node so it doesn't need to be shipped or shuffled between nodes with each stage.**

E. A broadcast variable is saved to the disk of each worker node to be easily read into memory when needed.

## Q75 - Which of the following operations is most likely to induce a skew in the size of your data's partitions?

A. DataFrame.collect()

B. DataFrame.cache()

C. DataFrame.repartition(n)

**D. DataFrame.coalesce(n)**

E. DataFrame.persist()

## Q76 - Which of the following data structures are Spark DataFrames built on top of?

A. Arrays

B. Strings

**C. RDDs**

D. Vectors

E. SQL Tables

## Q77 - Which of the following code blocks returns a DataFrame containing only column storeId and column division from DataFrame storesDF?

A. storesDF.select("storeId").select("division")

B. storesDF.select(storeId, division)

**C. storesDF.select("storeId", "division")**

D. storesDF.select(col("storeId", "division"))

E. storesDF.select(storeId).select(division)

## Q78 - The below code shown block contains an error. The code block is intended to return a DataFrame containing only the rows from DataFrame storesDF where the value in DataFrame storesDF's "sqft" column is less than or equal to 25,000. Assume DataFrame storesDF is the only defined language variable. Identify the error.

*Code block:* `storesDF.filter(sqft <= 25000)`

A. The column name sqft needs to be quoted like storesDF.filter("sqft" <= 25000).

**B. The column name sqft needs to be quoted and wrapped in the col() function like storesDF.filter(col("sqft") <= 25000).**

C. The sign in the logical condition inside filter() needs to be changed from <= to >.

D. The sign in the logical condition inside filter() needs to be changed from <= to >=.

E. The column name sqft needs to be wrapped in the col() function like storesDF.filter(col(sqft) <= 25000)

## Q79 - Which of the following operations can be used to convert a DataFrame column from one type to another type?

**A. col().cast()**

B. convert()

C. castAs()

D. col().coerce()

E. col()

## Q80 - Which of the following code blocks returns a new DataFrame with a new column sqft100 that is 1/100th of column sqft in DataFrame storesDF? Note that column sqft100 is not in the original DataFrame storesDF.

A. storesDF.withColumn("sqft100", col("sqft") * 100)

B. storesDF.withColumn("sqft100", sqft / 100)

C. storesDF.withColumn(col("sqft100"), col("sqft") / 100)

**D. storesDF.withColumn("sqft100", col("sqft") / 100)**

E. storesDF.newColumn("sqft100", sqft / 100)

## Q81 - Which of the following code blocks returns a new DataFrame from DataFrame storesDF where column numberOfManagers is the constant integer 1?

A. storesDF.withColumn("numberOfManagers", col(1))

B. storesDF.withColumn("numberOfManagers", 1)

**C. storesDF.withColumn("numberOfManagers", lit(1))**

D. storesDF.withColumn("numberOfManagers", lit("1"))

E. storesDF.withColumn("numberOfManagers", IntegerType(1))

## Q82 - Which of the following operations can be used to split an array column into an individual DataFrame row for each element in the array?

A. extract()

B. split()

**C. explode()**

D. arrays_zip()

E. unpack()

## Q83 - Which of the following code blocks returns a new DataFrame where column storeCategory is an all-lowercase version of column storeCategory in DataFrame storesDF? Assume DataFrame storesDF is the only defined language variable.

**A. storesDF.withColumn("storeCategory", lower(col("storeCategory")))**

B. storesDF.withColumn("storeCategory", col("storeCategory").lower())

C. storesDF.withColumn("storeCategory", tolower(col("storeCategory")))

D. storesDF.withColumn("storeCategory", lower("storeCategory"))

E. storesDF.withColumn("storeCategory", lower(storeCategory))

## Q84 -- The code block shown below contains an error. The code block is intended to return a new DataFrame where column division from DataFrame storesDF has been renamed to column state and column managerName from DataFrame storesDF has been renamed to column managerFullName. Identify the error.

`(storesDF.withColumnRenamed("state", "division").withColumnRenamed("managerFullName", "managerName"))`

A. Both arguments to operation withColumnRenamed() should be wrapped in the col() operation.

B. The operations withColumnRenamed() should not be called twice, and the first argument should be ["state", "division"] and the second argument should be ["managerFullName", "managerName"].

C. The old columns need to be explicitly dropped.

**D. The first argument to operation withColumnRenamed() should be the old column name and the second argument should be the new column name.**

E. The operation withColumnRenamed() should be replaced with withColumn().

## Q85 - Which of the following code blocks returns a DataFrame where rows in DataFrame storesDF containing missing values in every column have been dropped?

A. storesDF.nadrop("all")

B. storesDF.na.drop("all", subset = "sqft")

C. storesDF.dropna()

D. storesDF.na.drop()

**E. storesDF.na.drop("all")**

*Explanation:* See [pyspark.sql.DataFrame.dropna](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.dropna.html)

## Q86 - The code block shown below should return a new DataFrame with the mean of column sqft from DataFrame storesDF in column sqftMean. Choose the response that correctly fills in the numbered blanks within the code block to complete this task.

*Code block:* `storesDF.__1__(__2__(__3__).alias("sqftMean"))`

**A. 1. agg 2. mean 3. col("sqft")**

B. 1. mean 2. col 3. "sqft"

C. 1. withColumn 2. mean 3. col("sqft")

D. 1. agg 2. mean 3. "sqft"

E. 1. agg 2. average 3. col("sqft")

## Q87 - Which of the following code blocks returns the sum of the values in column sqft in DataFrame storesDF grouped by distinct value in column division?

A. storesDF.groupBy.agg(sum(col("sqft")))

B. storesDF.groupBy("division").agg(sum())

C. storesDF.agg(groupBy("division").sum(col("sqft")))

D. storesDF.groupby.agg(sum(col("sqft")))

**E. storesDF.groupBy("division").agg(sum(col("sqft")))**

## Q88 - Which of the following code blocks returns a DataFrame containing summary statistics only for column sqft in DataFrame storesDF?

A. storesDF.summary("mean")

**B. storesDF.describe("sqft")**

C. storesDF.summary(col("sqft"))

D. storesDF.describeColumn("sqft")

E. storesDF.summary()

## Q89 - Which of the following operations can be used to sort the rows of a DataFrame?

**A. sort() and orderBy()**

B. orderby()

C. sort() and orderby()

D. orderBy()

E. sort()

## Q90 - The code block shown below contains an error. The code block is intended to return a 15 percent sample of rows from DataFrame storesDF without replacement. Identify the error.

*Code block:* `storesDF.sample(True, fraction = 0.15)`

A. There is no argument specified to the seed parameter

B. There is no argument specified to the withReplacement parameter.

C. The sample() operation does not sample without replacement — sampleby() should be used instead.

D. The sample() operation is not reproducible.

**E. The first argument True sets the sampling to be with replacement.**

*Explanation:* See [pyspark.sql.DataFrame.sample](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.sample.html)

## Q91 - Which of the following operations can be used to return the top n rows from a DataFrame?

A. DataFrame.n()

**B. DataFrame.take(n)**

C. DataFrame.head

D. DataFrame.show(n)

E. DataFrame.collect(n)

## Q92 - Which of the following lines of code prints the schema of a DataFrame?

A. print(storesDF)

B. storesDF.schema

C. print(storesDF.schema())

**D. DataFrame.printSchema()**

E. DataFrame.schema()

## Q93 - In what order should the below lines of code be run in order to create and register a SQL UDF named "ASSESS_PERFORMANCE" using the Python function assessPerformance and apply it to column customerSatistfaction in table stores?

Lines of code:
  1. spark.udf.register("ASSESS_PERFORMANCE", assessPerformance)
  2. spark.sql("SELECT customerSatisfaction, assessPerformance(customerSatisfaction) AS result FROM stores")
  3. spark.udf.register(assessPerformance, "ASSESS_PERFORMANCE")
  4. spark.sql("SELECT customerSatisfaction, ASSESS_PERFORMANCE(customerSatisfaction) AS result FROM stores")

A. 3, 4

**B. 1, 4**

C. 3, 2

D. 2

E. 1, 2

## Q94 - In what order should the below lines of code be run in order to create a Python UDF assessPerformanceUDF() using the integer-returning Python function assessPerformance and apply it to column customerSatisfaction in DataFrame storesDF?

Lines of code:
  1. assessPerformanceUDF = udf(assessPerformance, IntegerType)
  2. assessPerformanceUDF = spark.register.udf("ASSESS_PERFORMANCE", assessPerformance)
  3. assessPerformanceUDF = udf(assessPerformance, IntegerType())
  4. storesDF.withColumn("result", assessPerformanceUDF(col("customerSatisfaction")))
  5. storesDF.withColumn("result", assessPerformance(col("customerSatisfaction")))
  6. storesDF.withColumn("result", ASSESS_PERFORMANCE(col("customerSatisfaction")))

**A. 3, 4**

B. 2, 6

C. 3, 5

D. 1, 4

E. 2, 5

## Q95 - Which of the following operations can execute a SQL query on a table?

A. spark.query()

B. DataFrame.sql()

**C. spark.sql()**

D. DataFrame.createOrReplaceTempView()

E. DataFrame.createTempView()

## Q96 - Which of the following code blocks creates a single-column DataFrame from Python list years which is made up of integers?

A. spark.createDataFrame([years], IntegerType())

**B. spark.createDataFrame(years, IntegerType())**

C. spark.DataFrame(years, IntegerType())

D. spark.createDataFrame(years)

E. spark.createDataFrame(years, IntegerType)

## Q97 - Which of the following operations can be used to cache a DataFrame only in Spark’s memory assuming the default arguments can be updated?

A. DataFrame.clearCache()

B. DataFrame.storageLevel

C. StorageLevel

**D. DataFrame.persist()**

E. DataFrame.cache()

*Explanation:* With `cache()`, we use only the default storage levels; 'MEMORY_ONLY' for RDD, and 'MEMORY_AND_DISK' for DataFrame. Use `persist()` to assign a storage level other than default storage levels.

## Q98 - The code block shown below contains an error. The code block is intended to return a new 4-partition DataFrame from the 8-partition DataFrame storesDF without inducing a shuffle. Identify the error.

*Code block:* `storesDF.repartition(4)`

A. The repartition operation will only work if the DataFrame has been cached to memory.

B. The repartition operation requires a column on which to partition rather than a number of partitions.

C. The number of resulting partitions, 4, is not achievable for an 8-partition DataFrame.

**D. The repartition operation induced a full shuffle. The coalesce operation should be used instead.**

E. The repartition operation cannot guarantee the number of result partitions.

## Q99 - Which of the following code blocks will always return a new 12-partition DataFrame from the 8-partition DataFrame storesDF?

A. storesDF.coalesce(12)

B. storesDF.repartition()

**C. storesDF.repartition(12)**

D. storesDF.coalesce()

E. storesDF.coalesce(12, "storeId")

## Q100 - Which of the following Spark config properties represents the number of partitions used in wide transformations like join()?

**A. spark.sql.shuffle.partitions**

B. spark.shuffle.partitions

C. spark.shuffle.io.maxRetries

D. spark.shuffle.file.buffer

E. spark.default.parallelism

## Q101 - In what order should the below lines of code be run in order to return a DataFrame containing a column openDateString, a string representation of Java’s SimpleDateFormat?

Note that column openDate is of type integer and represents a date in the UNIX epoch format — the number of seconds since midnight on January 1st, 1970. An example of Java's SimpleDateFormat is "Sunday, Dec 4, 2008 1:05 PM".

Lines of code:
  1. storesDF.withColumn("openDateString", from_unixtime(col("openDate"), simpleDateFormat))
  2. simpleDateFormat = "EEEE, MMM d, yyyy h:mm a"
  3. storesDF.withColumn("openDateString", from_unixtime(col("openDate"), SimpleDateFormat()))
  4. storesDF.withColumn("openDateString", date_format(col("openDate"), simpleDateFormat))
  5. storesDF.withColumn("openDateString", date_format(col("openDate"), SimpleDateFormat()))
  6. simpleDateFormat = "wd, MMM d, yyyy h:mm a"

A. 2, 3

**B. 2, 1**

C. 6, 5

D. 2, 4

E. 6, 1

## Q102 - Which of the following code blocks returns a DataFrame containing a column month, an integer representation of the month from column openDate from DataFrame storesDF?

Note that column openDate is of type integer and represents a date in the UNIX epoch format — the number of seconds since midnight on January 1st, 1970.

A. storesDF.withColumn("month", getMonth(col("openDate")))

**B. storesDF.withColumn("openTimestamp", col("openDate").cast("Timestamp")).withColumn("month", month(col("openTimestamp")))**

C. storesDF.withColumn("openDateFormat", col("openDate").cast("Date")).withColumn("month", month(col("openDateFormat")))

D. storesDF.withColumn("month", substr(col("openDate"), 4, 2))

E. storesDF.withColumn("month", month(col("openDate")))

## Q103 - Which of the following operations performs an inner join on two DataFrames?

A. DataFrame.innerJoin()

**B. DataFrame.join()**

C. Standalone join() function

D. DataFrame.merge()

E. DataFrame.crossJoin()

## Q104 - Which of the following code blocks returns a new DataFrame that is the result of an outer join between DataFrame storesDF and DataFrame employeesDF on column storeId?

**A. storesDF.join(employeesDF, "storeId", "outer")**

B. storesDF.join(employeesDF, "storeId")

C. storesDF.join(employeesDF, "outer", col("storeId"))

D. storesDF.join(employeesDF, "outer", storesDF.storeId == employeesDF.storeId)

E. storesDF.merge(employeesDF, "outer", col("storeId"))

## Q105 - The below code block contains an error. The code block is intended to return a new DataFrame that is the result of an inner join between DataFrame storesDF and DataFrame employeesDF on column storeId and column employeeId which are in both DataFrames. Identify the error.

*Code block:* `storesDF.join(employeesDF, [col("storeId"), col("employeeId")])`

A. The join() operation is a standalone function rather than a method of DataFrame — the join() operation should be called where its first two arguments are storesDF and employeesDF.

B. There must be a third argument to join() because the default to the how parameter is not "inner".

C. The col("storeId") and col("employeeId") arguments should not be separate elements of a list — they should be tested to see if they're equal to one another like col("storeId") == col("employeeId").

D. There is no DataFrame.join() operation — DataFrame.merge() should be used instead.

**E. The references to "storeId" and "employeeId" should not be inside the col() function — removing the col() function should result in a successful join.**

## Q106 - Which of the following Spark properties is used to configure the broadcasting of a DataFrame without the use of the broadcast() operation?

**A. spark.sql.autoBroadcastJoinThreshold**

B. spark.sql.broadcastTimeout

C. spark.broadcast.blockSize

D. spark.broadcast.compress

E. spark.executor.memoryOverhead

## Q107 - The code block shown below contains an error. The code block is intended to write DataFrame storesDF to file path filePath as parquet and partition by values in column division. Identify the error.

*Code block:* `storesDF.write.repartition("division").parquet(filePath)`

A. The argument division to operation repartition() should be wrapped in the col() function to return a Column object.

B. There is no parquet() operation for DataFrameWriter — the save() operation should be used instead.

**C. There is no repartition() operation for DataFrameWriter — the partitionBy() operation should be used instead.**

D. DataFrame.write is an operation — it should be followed by parentheses to return a DataFrameWriter.

E. The mode() operation must be called to specify that this write should not overwrite existing files.

## Q108 - Which of the following code blocks reads JSON at the file path filePath into a DataFrame with the specified schema schema?

A. spark.read().schema(schema).format(json).load(filePath)

B. spark.read().schema(schema).format("json").load(filePath)

C. spark.read.schema("schema").format("json").load(filePath)

D. spark.read.schema("schema").format("json").load(filePath)

**E. spark.read.schema(schema).format("json").load(filePath)**
