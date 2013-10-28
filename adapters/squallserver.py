#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#
# TODO: Group(), Having(), Union(), Except(), Intersect()
#

'''
From the wiki of the pyodbc project:

http://code.google.com/p/pyodbc/wiki/ConnectionStrings
-->
On Windows

There are actually two or three SQL Server drivers written and distrubuted by Microsoft: one referred to as "SQL Server" and the other as "SQL Native Client" and "SQL Server Native Client 10.0}".

    DRIVER={SQL Server};SERVER=cloak;DATABASE=test;UID=user;PWD=password
    DRIVER={SQL Native Client};SERVER=dagger;DATABASE=test;UID=user;PWD=password
    DRIVER={SQL Server Native Client 10.0};SERVER=dagger;DATABASE=test;UID=user;PWD=password
<-- 
'''
import sys, os
sys.path.append(os.path.join('..'))

from squall import Sql, Verbatim, Select, Condition, Field
from squallerrors import *
import pyodbc
import datetime as dt

class SqlAdapter(object):
    '''
    API for calling odbc (sql server)
    Expects the odbc module as module parameter
    '''

    
    
    
    _instance = None
    def __new__(self, *args, **kwargs):
        '''
        :Description:
            Singleton pattern that instantiates an SqlAdapter object
            
        :Parameters:
            - *args: iterable position-based parameters, received from 
                     squallsql SqlAdapter object init.
                     (Currently do nothing here)
                     
            - **kwargs: dictionary of parameters, received from 
                        squallsql SqlAdapter object init.
                        (Currently do nothing here)
        '''
        if not self._instance:
            self._instance = super().__new__(self)
        return self._instance
    
    def __init__(self, *args, **kwargs):
        '''
        :Description:
            Initialization of database-specific SqlAdapter
            
            > Parse the kwarg 'database' keyword to set the sql server 
              database, otherwise it will use 'master' by default.
        '''
        self.database = kwargs.get('database', 'master') 
        
    def sql_compat(self, sql, param=()):
        '''
        Compatibility (temporary) sql method to force return of rows in 
        the execute call. 
        No transaction capabilities in this method.
        '''
        self.cursor.execute(sql, param)
        return self.cursor.fetchall()
            
    def connect(self, *args, **kwargs):
        '''
        :Parameters:
            - **kwargs: dictionary; contains list of parameters for connections
                - database: name of database. If left empty or None, will default
                            to 'master'
                - driver: string; name of sql server (or odbc driver)
                - db_host: string; hostname, can be localhost or remote address
                - uid: string; User identifier for connection
                - pwd: string; Password for user
                - trusted: boolean; Whether to assume user's current credentials
                  INSTEAD of a username/password pair for login to connection
                - dsn: string; (NOT IMPLEMENTED)
                - dbq: string; microsoft access database file (NOT IMPLEMENTED)
        '''
        #TODO: Connection checking
        
        self.connection_str = []
        # Assume sqlserver driver
        self.driver = 'SQL Server'
        # TODO:
        #     Connect to odbc
#         self.conn = self.module.connect(conn_str) 
#         self.cursor = self.conn.cursor() # We need this cursor in the class
#         return self.conn
        # self.cursor = self.conn.cursor()
        self.connection_str.append('DRIVER={{{}}}'.format(self.driver))
        self.db_host = kwargs.get('db_host', 'localhost')
        self.database = kwargs.get('database', 'master')
        self.connection_str.append('DATABASE={}'.format(self.database))
        
        if kwargs.get('trusted', False):
            self.connection_str.append('Trusted_Connection=yes')
            
        if not kwargs.get('uid') is None:
            self.connection_str.append(kwargs.get('uid'))
        if not kwargs.get('pwd') is None:
            self.connection_str.append('PWD={}'.format(kwargs.get('pwd')))
        if not kwargs.get('server') is None:
            self.connection_str.append('SERVER={}'.format(kwargs.get('server')))
        
            
        # Converts array to string separated by ; characters into configuration
        conn_str = ';'.join(self.connection_str)
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

#     
    def disconnect(self):
        '''
        :Description:
            Disconnect the driver from the database.
        '''
        self.conn.close()
    
    def sql(self, sql, param=()):
        '''
        :Description:
            Executes the sql string
            
        :Parameters:
            - sql: string; sql statement
        '''
        self.cursor.execute(sql, param)
        return self.conn
    
    def commit(self):
        '''
        Deprecated in favour of Transaction objects
        :Description:
            Calls the commit() function of the database connection
            driver.
        '''
        self.conn.commit()
        
    def rollback(self):
        '''
        Deprecated in favour of Transaction objects
        :Description:
            Causes a rollback call to be emitted, causing all
            statements up to the current point to be rolled back
        '''
        self.conn.rollback()
        
    class transaction(Sql):
        '''
        Sql Server Transaction object
        
        :Description:
            Manages transactions from an sqlserver-specific perspective.
            
        :Parameters:
            - adapter: object; sql database adapter object
            - *args: list; arguments containing sql objects to add to transaction
            - **kwargs: dict; specific arguments for managing the sql server transaction
                - name: string; what to name the transaction statement ??
                - command: string; COMMIT or ROLLBACK
                - adapter: object; refers to this SqlAdapter instance
                - 
        '''
        
        def __init__(self, *args, **kwargs):
            self.adapter = kwargs.get('adapter', SqlAdapter._instance)
            self.tname = kwargs.get("name", "Default Transaction")

#    ['USE {};'.format(kwargs.get('database',
#    SqlAdapter().database)]
            self.tpreamble = ['BEGIN TRANSACTION {}'.format(self.tname)]
            self.tobjects = []
            self.tobjects.extend(args)
            self.rollbackstring = 'SET xact_abort ON;'
            self.tdefaultcmd = kwargs.get('command', 'COMMIT')
            self.tsuffix = '{} {} {}'.format(self.tdefaultcmd, 'TRANSACTION', 
                                             self.tname)
            if self.tdefaultcmd == 'ROLLBACK':
                # Format string to set rollback on incorrect statement
                # during transaction
                self.tpreamble = '{}\n{}'.format(self.rollbackstring, 
                                                 self.tpreamble)
            self.output = {}
            # Tried using a generator, the generator got added
            
        def add(self, *args):
            '''
            :Returns:
                - args: list; all sqlobjects that were provided as arguments
            '''
            for a in args:
                if not isinstance(a, Sql):
                    if isinstance(a, str):
                        self.tobjects.append(Verbatim(a))
                        continue
                    else:
                        raise InvalidSquallObjectException(
                            'Cannot add non-sql object {}'.format(str(a)))
                self.tobjects.append(a)
            return args
                
        def clear(self):
            '''
            :Returns:
                All the transaction's current sql output from selects
            '''
            output = self.output
            self.output = {}
            self.tobjects = []
            return output
        
        def run(self, *args, **kwargs):
            '''
            :Description:
                Goes through every Squall Command object and runs the sql through
                the adapter object supplied. (When an adapter class overrides this
                method, it should supply the adapter automatically, so only args
                need to be supplied.)
                Raises an exception and attempts to rollback when an exception
                is found, meaning an error arose during sql execution.
                commit() is called if no errors during execution occur.
                
            :Parameters:
                - **kwargs: dict; 
                    - rollback_callback: If a rollback is called, call this method
                    - success_callback: If Transaction completes as expected, call this
                      method
                    - raise_exception: boolean; raise exceptions on execution completion
                      or error. (great for stricter environments) This does mean that
                      no return statements will be called unless embedded into the error
                      message or object. 
                    - force: either "commit" or "rollback" is acceptable.
                            
            :Exceptions:
                - EmptyTransactionException: Called when *args is empty and nothing
                  can be run
                - RollbackException: when raise_exception is True, committing and
                  rolling back will raise an exception. This is raised when a 
                  rollback is encountered.
                - CommitException: when raise_exception is True, committing and
                  rolling back will raise an exception. This is raised when a 
                  commit is successful.
                - InvalidSquallObjectException - If at any point an AdapterException
                  is raised during execution of sql objects.
                  
            :Returns:
                - None if rollback occured and transaction failed,
                - list if successful commit, list contains all transaction objects
            '''
            #Handle force cases first
            if kwargs.get('force') == 'rollback':
                self.clear()
                self.adapter.rollback()
            elif kwargs.get('force') == 'commit':
                self.adapter.commit()
                
            if len(self.tobjects) == 0:
                raise EmptyTransactionException('No objects to execute')
            for tobj in self.tobjects:
                if not isinstance(tobj, Sql):
                    raise InvalidSquallObjectException('{} is invalid'.format(
                        str(tobj)))
            
            
            # Of Sql() in transaction queue, get output of selects, run others
            for squallobj in self.tobjects:
                if isinstance(squallobj, Select):
                    self.output[str(squallobj)] = self.adapter.sql_compat(str(squallobj))
                else:
                    self.adapter.sql(str(squallobj))
            self.adapter.commit()
            
            if not kwargs.get('raise_exception') is None:
                raise CommitException('Committed Transaction')
            return self.clear()
            
            
        def __repr__(self):
            ret = []
            ret.extend(self.tpreamble)
            
            # Append statements here
            ret.extend(str(x) for x in self.tobjects)
            
            ret.append(self.tsuffix)
            return '\n'.join(ret)
        
    class Fields(Sql):
        '''
        :Description:
            Fields is an object that is a collection of one or more Field() 
            objects or column-name strings. 
        '''
        
        def __init__(self, *args, **kwargs):
            self.fields = args
            
        def __repr__(self):
            return '{}'.format(', '.join(str(x) for x in self.fields))
        
    class Field(Condition):
        '''
        :Description:
            Field is an object that contains the column name, the type, and
            any associated Key() objects. Used mainly in the context of
            Create() objects, not other Sql objects although if colname is
            the only parameter specified, it works.
            
        :Parameters:
            Required
            - colname; string: Name of the column.
            Optional 
            - kwargs:
                - datatype; string: Name of Data Type that SQL database supports
                - key; Key(): Primary Key or Foreign Key objects
                - nullable; bool: Whether field can be null or not null
        '''
        
        def __init__(self, colname, *args, datatype=None, key=None, 
                     nullable=True, **kwargs):
            self.colname = colname
            self.datatype = datatype
            self.key = key
            self.null = nullable
        
        def __repr__(self):
            null = ' NULL' if self.null == True else ' NOT NULL'
            datatype = ' {}'.format(self.datatype) if self.datatype else ''
            key = ' {}'.format(str(self.key)) if self.key else ''
            return '{}{}{}{}'.format(self.colname, datatype, 
                                    null, key)
        
        
    class Value(Sql):
        '''
        :Description:
            Value() is a way to ensure the database types are met.
            I am making an executive decision to force database types
            into native python types and vice versa.
            Therefore, if you make a Select command using squall, in the
            results it will return a pythonic date object (datetime module)
            instead of a string. Likewise, when calling an Insert() object,
            the value object should be a datetime object, which will get
            converted to the appropriate string or methodcall by the database
            adapter.
            
            For instance: if type(value) == str in python,
                output: \'\'\' 'value' \'\'\' (quoted)
            
            if type(value) == datetime in python, 
                output: \'\'\'strftime('%Y-%m-%d %H:%M:%S', '2004-01-01 02:34:56')\'\'\'
            
            where '2004-01-01 02:34:56' is the output of a str(datetime) object type
        '''
        def __init__(self, val, *args, **kwargs):
            '''
            :Parameters:
                - val: any type; Value() object is a container around this attribute 
                - **kwargs; dict:
                    - forcetype; string: SQL Server Data Type (INT, DATE, VARCHAR, etc. etc.)
                      TODO: Dictionary with Type Values instead of manual entry
                      (Use with Create() objects) 
                    - null; bool or None: force NULL or NOT NULL attributes if True or False, 
                      unless None is specified. Converts argument to a bool, so Integers can
                      be used to specify whether NULL or NOT NULL.
                      (Use with Create() objects)
            '''
            if isinstance(val, str):
                val = """'{}'""".format(val)
            self.value = val
            self.null = kwargs.get('null', None)
            if not self.null is None:
                self.null = bool(self.null)
            self._type = kwargs.get('forcetype', '')
            
        def __repr__(self):
            null = ''
            if not self.null is None:
                null = ' NULL' if self.null == True else ' NOT NULL'
            typ = ' ' + self._type if True else '' 
            return '{}{}{}'.format(self.value, typ, null)
        
    class Exists(Sql):
        
        def __init__(self, exists, selector, statement, *args, Else=None):
            '''
            :Description:
                The Exists() object is written in a more readable fashion and
                acts similar to an if else statement.
                
            :Parameters:
                - selector; string: A Select() or Table() object
                - statement; Sql() Statement: Command to run if Exists 
                - **kwargs:
                    - Else: Sql() statement if exists bool equals False
                    
            '''
            self.exists=exists
            self.selector = selector
            self.statement = statement
            
        def __repr__(self):
            exists = " " if self.exists else " NOT "
            #vobj = Verbatim("""IF NOT EXISTS(SELECT * FROM sys.tables WHERE name = 't') CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))""")
            return "IF{}EXISTS({}) {}".format(exists, self.selector, self.statement)
    