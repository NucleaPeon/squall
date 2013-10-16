pysquall
====

Database Adapter for multiple sql vendors, one command fits all.

Some functionality of the RDBMS may be squelched for massive
compatibility of all relational databases.

As with most object-oriented platforms, you can expect to see a 
slight performance hit in exchange for the additional functionality
and ease of use.


How to use this software
----
See the GitHub wiki page for getting database adapters to work:
https://github.com/NucleaPeon/squall/wiki

Recent Activity
====

Working on allowing adapters to have their own specific versions
of Sql objects. Primary Key and Create() objects work on sqlserver
and many more objects such as Field() have been implemented on the
same driver, with empty classes ported to squall.py.

Lots of work to do still.

Databases
====
A list of databases I want to utilize in this adapter software are:

* sqlite3 - successfully written, basic tests implemented
* sqlserver - current driver project
* mysql
* postgres
* firebird


This software should have contributers from all database systems
that want to contribute.

Functionality that must be sustained:

* SELECT
* INSERT
* UPDATE
* DELETE
* DROP
* CREATE
* WHERE
* HAVING
* ORDER
* 

This software aims to be a solid single-threaded application first and 
foremost. Multi-threading can be implemented at a later date.

Sql Server (Currently Under Construction)
---------
```
import squall
self.sqlobj = squall.Session().connect('myDatabase', adapter='sqlserver', trusted=True, driver='SQL Server')
self.module = squall.db('sqlserver')
```

( PLEASE NOTE THAT USING USERNAME AND PASSWORD NOT TESTED, BUT MAY FUNCTION... )

OR

Sqlite3 
---------
```
import squallsql
driver = squallsql.SqlAdapter(driver='squallsqlite3', 
                              database='/path/to/database.db')
```

( driver will automatically attempt a connection )

When specifying the "driver" kwarg, the value would be the module name of the adapter class that reflects the db you want to use.
 
Notes on Value objects
----------
It is important to note that when filling out (for example) a Where() object,
if you do not put the value into a Value() object, you will not get quotes on the
object.

```
# WRONG
w = Where('field', '=', someIdentifier)

# RIGHT
w = Where('field', '=', Value(someIdentifier))

# ALSO RIGHT
w = Where(Fields('field'), '=', Value(someIdentifier))

# Another Variation
# Where ___ IN ___ sql statements require the values to be formatted in () brackets 
# and with commas, so WhereIn is an easy-to-use shortcut to do those Where comparisons
w = WhereIn(Fields('Id'), ['a', 'b', 'c', 'd', 'e']) # Can be python-native list
```
