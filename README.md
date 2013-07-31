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
* (if applicable) Change Database on the Fly -- sqlite3 requires the path.
* Safe Transactions (rollback on error)

This software aims to be a solid single-threaded application first and 
foremost. Multi-threading can be implemented at a later date.

How to use this software
----

Sql Server
---------
```
import squall

self.s = squall.Session('sqlserver', 'rfid')
self.module = squall.db('sqlserver') # module contains connection methods
self.sqlobj = squall.ADAPTERS['sqlserver'] # sets the sql object which contains update/insert/delete/select methods
self.sqlobj.connect('yourdb', trusted=True, driver='SQL Server') # Connects to sqlserver with generic driver using your own credentials
```

( PLEASE NOTE THAT USING USERNAME AND PASSWORD NOT TESTED, BUT MAY FUNCTION... )

OR

Sqlite3 
---------
```
self.s = squall.Session('sqlite3', 'rfid.db')
self.module = squall.db('sqlite3')
self.sqlobj = squall.ADAPTERS['sqlite3']
self.sqlobj.connect('relative/or/absolute/path/to/database.db') # will create if not found
```
 
