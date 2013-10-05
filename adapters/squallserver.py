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

from squall import *
from squallerrors import *
import pyodbc

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
#         if not kwargs.get('db_name', None):
#             raise AdapterException(
#                 'Database not supplied to driver, cannot connect')
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
    
    def insert(self, sqlobject, precallback=None, postcallback=None):
        '''
        :Description:
            Database Adapter insert->sql method with callbacks for
            added functionality. This allows users to support logging
            and additional commits.
        
        :See:
            Transaction:
                Since insert/update/delete require commit to perform, 
                multiple methods can be placed into a transaction and
                can all be commited at once.
                
                In order to utilize this functionality, callbacks are 
                required. Use the postcallback() to return the sql string.
            
        :Parameters:
            Parameters that are submitted to both callback methods are
            as follows:
            - method: this insert method is supplied
            - module: this class so that multiple methods can be strung 
              together
              main non-query is submitted
            - sql: this is the sql structure object or string that contains
              the fields, tables and conditions for the statement
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            precallback(**{'method':self.insert, 'class':self, 
                           'sql':sqlobject})
        self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback(**{'method':self.insert, 'class':self, 
                           'sql':sqlobject})
        return self.conn
    
    def update(self, sqlobject, precallback=None, postcallback=None):
        '''
        :Description:
            Database Adapter update->sql method with callbacks for
            added functionality. This allows users to support logging
            and additional commits.
        
        :See:
            Transaction:
                Since insert/update/delete require commit to perform, 
                multiple methods can be placed into a transaction and
                can all be commited at once.
                
                In order to utilize this functionality, callbacks are 
                required. Use the postcallback() to return the sql string.
            
        :Parameters:
            Parameters that are submitted to both callback methods are
            as follows:
            - method: this insert method is supplied
            - module: this class so that multiple methods can be strung 
              together
              main non-query is submitted
            - sql: this is the sql structure object or string that contains
              the fields, tables and conditions for the statement
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            precallback()
        self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def select(self, sqlobject, precallback=None, postcallback=None):
        '''
        :Description:
            Database Adapter insert->sql method with callbacks for
            added functionality. This allows users to support logging
            and additional commits.
        
        :See:
            Transaction:
                Since insert/update/delete require commit to perform, 
                multiple methods can be placed into a transaction and
                can all be commited at once.
                
                Selects don't normally require a transaction and nothing
                is committed, but it is still recommended to use selects
                in transaction objects.
                
                In order to utilize this functionality, callbacks are 
                required. Use the postcallback() to return the sql string.
            
        :Parameters:
            Parameters that are submitted to both callback methods are
            as follows:
            - method: this insert method is supplied
            - module: this class so that multiple methods can be strung 
              together
              main non-query is submitted
            - sql: this is the sql structure object or string that contains
              the fields, tables and conditions for the statement
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            precallback()
        self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def delete(self, sqlobject, precallback=None, postcallback=None):
        '''
        :Description:
            Database Adapter delete->sql method with callbacks for
            added functionality. This allows users to support logging
            and additional commits.
        
        :See:
            Transaction:
                Since insert/update/delete require commit to perform, 
                multiple methods can be placed into a transaction and
                can all be commited at once.
                
                In order to utilize this functionality, callbacks are 
                required. Use the postcallback() to return the sql string.
            
        :Parameters:
            Parameters that are submitted to both callback methods are
            as follows:
            - method: this insert method is supplied
            - module: this class so that multiple methods can be strung 
              together
              main non-query is submitted
            - sql: this is the sql structure object or string that contains
              the fields, tables and conditions for the statement
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            precallback()
        self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback()
        return self.conn
    
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
            if kwargs.get('db_name') is None:
                self.tpreamble = ['BEGIN TRANSACTION {}'.format(self.tname)]
            else:
                self.tpreamble = ['USE {};'.format(kwargs.get('db_name')), 
                              'BEGIN TRANSACTION {}'.format(self.tname)]
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
            self.cmds = self.tpreamble
            self.output = {}
            # Tried using a generator, the generator got added
            for x in self.tobjects:
                self.cmds.append(str(x))
            self.cmds.append(self.tsuffix)
            
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
                
            for squallobj in self.tobjects:
                if isinstance(squallobj, Select):
                    self.output[str(squallobj)] = self.adapter.sql_compat(str(squallobj))
                else:
                    self.adapter.sql(str(squallobj)) # This will raise a rollback exception 
                # via sqlite3, so we don't have to check for this. Other db's will have
                # to reimplement this.
            self.adapter.commit()
            
            if not kwargs.get('raise_exception') is None:
                raise CommitException('Committed Transaction')
            return self.clear()
            
            
        def __repr__(self):
            return '\n'.join(self.cmds)

    
class Create(Sql):
    '''
    :Description:
        Sql Object for creating Tables in the database
        
    :Parameters:
        - table: Sql(); Table object
        - fields: Sql(); Fields in the Table to create
        - constraints: Sql(); Constraint object on a Field
    
    TODO: Implement exists in repr() for both drop and create
    '''
    
    def __init__(self, table, fields, constraints = [], exists=None, **kwargs):
        self.table = table
        self.fields = fields
        self.constraints = constraints
        self.exists = exists
        
    def __repr__(self):
        return "CREATE TABLE {}({}{})".format(self.table, 
                                              ', '.join(self.fields),
                                              ', '.join(self.constraints))
        
        
class Drop(Sql):
    '''
    :Description:
        Sql Object for dropping Tables in the database
        
    :Parameters:
        - table: Sql(); Table object
        
    '''
    
    def __init__(self, table, exists=None, **kwargs):
        self.table = table
        self.exists = exists
         
    def __repr__(self):
        return "DROP TABLE {}".format(self.table)
    
class Exists(Condition):
    '''
    :Description:
        Sql Condition that determines whether statements should be run based 
        on the existance of certain elements, such as a Table, Field or Value.
        
        SqlServer Exist conditions are executed in a query before the main
        queries are executed like so:
            - If [not] exists (...select...query...) [do statements here]
            
        Exist() for SqlServer objects expects that the Sql Statements
        (Update(), Delete(), Select(), Insert(), Drop() and Create()) will
        check for Exists() conditions specifically in their __repr__
        methods and surround the statement properly. This is not like sqlite3
        Exists which trails the sql statement.
        
    :Parameters:
        - exists; bool: If True, check that condition exists, otherwise check
                        its absence (using NOT keyword)
        - conditions; Condition: Addition Condition objects for more precise
                                 existential checking
    '''
    
    
    def __init__(self, exists=True, conditions = []):
        self.exists = exists
        self.conditions = conditions
        
    def __repr__(self):
        ifnot = "NOT " if not self.exists else '' 
        return "IF {}EXISTS".format(ifnot)
    
    