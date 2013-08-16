pysquall
====

Database Adapter for multiple sql vendors, one command fits all.

Some functionality of the RDBMS may be squelched for massive
compatibility of all relational databases.

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

This software aims to be a solid single-threaded application first and 
foremost. Multi-threading can be implemented at a later date.

How to use this software
----

Sql Server
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
                                  database='rfid.db')
```

( driver will automatically attempt a connection )
 
