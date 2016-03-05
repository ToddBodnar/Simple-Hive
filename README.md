# Simple-Hive
An imitation of Apache Hive built from scratch using Java and Hadoop.

## How do I run this?
Download the most recent release or build it using Maven by running `mvn clean compile assembly:single` in the main directory.

Simple Hive supports both local and distributed queries. From an end user's perspective, there isn't much of a difference aside from local mode not requiring access to a Hadoop/HDFS environment.

### Locally

You can start an interactive session using `java -jar simple-hive-VERSION.jar --local -D DATABASE_NAME`. The current build contains two demo databases: `Star_Trek` and `BSG_Game_Info`. You can view the list of tables by running `show tables` and get a description of each table using `describe TABLE_NAME`. 

### Using Hadoop
You can start an interactive session using `java -jar simple-hive-VERSION.jar -D DATABASE_NAME`. The current build contains two demo databases: `Star_Trek` and `BSG_Game_Info`. You can view the list of tables by running `show tables` and get a description of each table using `describe TABLE_NAME`. 

If a table is not stored on HDFS, then Simple Hadoop will copy them to HDFS during the job's execution. Specifically, they are stored in the format LOCAL_TABLE_timestamp_###. Results from a job, including intermediary results for chained jobs are stored on HDFS in the form /TEMP_TABLE_timestamp/ regardless of where the initial tables originated from.


## Full Language Support
Simple Hive supports SQL queries similar to `select name, shipname as Assignment from people join ships on ship = shipID` or `select name, age from people where ship = 3 and age > 25`. Also, you can generate statistics by running queries like `colstats age from people` or `colstats age group by ship from people`.

You can also use `help`,`quit`,`exit`, `show tables` and `describe TABLE_NAME`. 



## Alternatives
You really shouldn't use this in a production environment. Check out sqlite, mysql, or hive if you're looking for a relational db.

## For Programmers
### Table Storage
Tables are stored in files described by the interface `com.toddbodnar.simpleHive.IO.file` with additional metadata (column names, separators, etc) in `com.toddbodnar.simpleHive.metastore.table`. 

Currently, there are four implementations of the file interface, all stored in `com.toddbodnar.simpleHive.IO`. `ramFile` stores data in the programs's memory and is used for intermediaries when running in local mode. `laggyRamFile` extends `ramFile` but includes a user-defined amount of lag for each query, mainly to be used for testing things. `fileFile` is for local files, for example the `players` table in `BSG_Game_Info` (stored in `testData/bsg_cards.csv`). `hdfsFile` supports files stored on HDFS. It also represents directories that contain multiple files (for example, output from a Hadoop job with multiple reducers) as a single file. `hdfsTable` also includes a helper function to transfer other files into HDFS.

### Job Format

Simple Hive includes a job wrapper, `MapReduceJob`, which is extended by `query` which includes some extra features specific to Simple-Hive jobs (i.e. table IO). We use a wrapper instead of just direct references to the mappers/reducers to simplify passing jobs between different parts of the program. An implemented query contains functions `getMapper` and `getReducer` which should return the Mapper and Reducer for that specific job. The Mapper and Reducer classes must be static to be executed on Hadoop (but they can be private). Functions get/set Input/Output are your standard getters and setters, but it is assumed that getOutput will create a new table if it is called before setOutput is. `getKeyType` and `getValueType` are used to set the class for the key/value pairs written by the mapper and read by the reducer. Finally, `writeConfig` is used to transfer the job's configuration from it's initial creation to wherever it is being executed. **Local variables set in the query class are not guaranteed to be transfered to the mapper or reducer.**

### Execution Flow

When a query is entered by the user, it is first passed to `com.toddbodnar.simpleHive.compiler.Parser` which then attempts to convert it into a DAG (directed, acyclic graph) of jobs to be executed. For example, `select * from a join b on c = d where e > 0` will create a three job graph visualized by:

[table b] ------v  
[table a] -> (a join b on c = d) -> (where e > 0) --> (select *) --> [output]

This DAG is represented by the `workflow` class. Starting with the root of the workflow, the program first checks if the job has any non-executed dependencies. If it does, it recursively processes the dependencies and sets the output of the dependencies to the input of the job. Otherwise, it just executes the job. 

Depending on the program's configuration, either `SimpleHadoopDriver` or `DistributedHadoopDriver` will be run to either emulate Hadoop locally or to execute the Hadoop job on your cluster. Each driver first pulls the job's configuration by running `MapReduceJob.writeConfig(Configuration conf)`. The Map/Reduce job is then executed.

If you want to bypass the parser or workflow and write your own code directly, check out `com.toddbodnar.simpleHive.helpers.playground` for some examples. This format may be more familiar to users of PIG or Spark where you, generally, store intermediaries in RDDs and run one Map/Reduce job per line.

### Testing

Most of the main classes have `JUnitTests` for them stored in the src/test directory. You can run them by running `mvn clean test`. There are two words of caution, however. First, all of these tests are run locally and *not* through Hadoop. This may change at a later date, but doing it this way makes testing a lot faster and avoids dependence on a Hadoop cluster. Second, if the tester cannot connect to HDFS, the `hdfsFile` tests will be skipped.

## TODO:

### Query parsing
I have a simplified parser working now, but I plan on replacing it with a full parser to build ASTs (see compiler.workflow).

### Persistant Metastore
Right now everything's stored in ram.

### CREATE and INSERT code
You can do this now (most of the queries result in new tables with inserted data), but it's not front-end friendly.
