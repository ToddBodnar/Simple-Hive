# Simple-Hive
An imitation of Apache Hive built from the ground up just for fun.

##What is this?
It's an immitation of Apache Hive but with less features and much less optimization.

##How do I run this?
You can start an interactive session using `ant compile jar run`. Right now the front end is running a limited parser (show tables, describe X, select, where, etc) which will later be replaced (see below). Those that are more comfortable with reading code should look at simpleHive.mrJobs.tests.playground for some toy examples of additional functionality that isn't yet supported by the parser.

Simple-Hive currently is built on a simple Hadoop emulator, so it can be run locally and it isn't necessary to have a Hadoop system set up.

##Why make this?
I thought it might be fun to build a map-reduce based db from the ground up.

##Alternatives
You really shouldn't use this in a production environment. Check out sqlite, mysql, or hive if you're looking for a relational db.

##TODO:

###Query parsing
I have a simplified parser working now, but I plan on replacing it with a full parser to build ASTs (see compiler.workflow).

###Persistant Storage
Right now everything's stored in ram.

###CREATE and INSERT code
You can do this now (most of the queries result in new tables with inserted data), but it's not front-end friendly.

###Full Hadoop support
Mainly build a wrapper for the Mapper/Reducer code for each of the queries. Will probably add HDFS support too. (Possibly HBase?)

###JUnit Testing
There's some basic tests implemented, but they should be ported to JUnit and expanded.
