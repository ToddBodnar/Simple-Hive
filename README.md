# Simple-Hive
An imitation of Apache Hive built from the ground up just for fun.

##What is this?
It's an immitation of Apache Hive but with less features and much less optimization.

##Why make this?
I thought it might be fun to build a map-reduce based db from the ground up.

##Alternatives
You really shouldn't use this in a production environment. Check out sqlite, mysql, or hive if you're looking for a relational db.

##What still needs to be done

###Query parsing
I have a little bit of a parser working for the "Where" clauses, but right now everything needs to be coded by hand. 

For example, if you'd want to run `select name,age from people`, right now you'd need to run

```
database d = database.getTestDB();
query q = new select("_col3,_col1");
q.setInput(d.getTable("people"));
hadoopDriver.run(q, true);
System.out.println(q.getResult());
```


Check out simpleHive/mrJobs/tests/playground.java for more examples.

###Persistant Storage
Right now everything's stored in ram.

###More functions
Right now, you can `select`, `left join` and `where`. Eventually, I'd like to also be able to `create table`, `insert into table`, do other joins and have the basic math operators (`count(*)`,`average(*)`,etc.) included.

###Network support
Maybe.
