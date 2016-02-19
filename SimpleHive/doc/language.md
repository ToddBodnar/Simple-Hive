#Simple Hive Language Specification

##Introduction
Just a bare bones spec, read it as `NODE -> (expands into) ANOTHER_NODE | (or) text`. So, for example, a simple language defined by `NUMBER -> number | NUMBER+NUMBER` would be able to encode strings like `2+4+5` or `1`.


##Transformations
`QUERY -> SELECT_QUERY | DESCRIBE_QUERY | SHOW_QUERY`

`SELECT_QUERY -> select SELECT_VALUES from TABLE [where BOOLEAN_TEST]`

`SELECT_VALUES -> * | SELECT_VALUE`

`SELECT_VALUE -> column_name | column_name as STRING | STRING | STRING as STRING | SELECT_VALUE, SELECT_VALUE`

`TABLE -> table_name`

`BOOLEAN_TEST -> BOOLEAN_TEST or BOOLEAN_TEST | BOOLEAN_TEST and BOOLEAN_TEST | column_name COMPARISON column_name | column_name COMPARISON STRING | STRING COMPARISON column_name | STRING COMPARISON STRING`

`COMPARISON -> = | != | >= | <= | > | <`

`DESCRIBE_QUERY -> describe table_name`

`SHOW_QUERY -> show tables`