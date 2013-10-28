squall
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

Working on sqlserver branch

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

How to use this software
----

Please read wiki entries for up-to-date information regarding usage.
https://github.com/NucleaPeon/squall/wiki
 
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
