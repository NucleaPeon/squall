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
import pyodbc

class SqlAdapter(object):
    '''
    API for calling odbc (sql server)
    Expects the odbc module as module parameter
    '''
    
    _instance = None
    def __new__(self, *args, **kwargs):
        if not self._instance:
            self._instance = super().__new__(self)
        return self._instance
        
    def connect(self, *args, **kwargs):
        '''
        :Parameters:
            - **kwargs: dictionary; contains list of parameters for connections
                - db_name: name of database
                - driver: string; name of sql server (or odbc driver)
                - db_host: string; hostname, can be localhost or remote address
                - uid: string; User identifier for connection
                - pwd: string; Password for user
                - trusted: boolean; Whether to assume user's current credentials
                  INSTEAD of a username/password pair for login to connection
                - dsn: string; 
                - dbq: string; microsoft access database file (NOT IMPLEMENTED)
        '''
        #TODO: Connection checking
        self.conn = None
        connection_str = []
        # Assume sqlserver driver
        driver = 'SQL Server'
        # TODO:
        #     Connect to odbc
#         self.conn = self.module.connect(conn_str) 
#         self.cursor = self.conn.cursor() # We need this cursor in the class
#         return self.conn
        # self.cursor = self.conn.cursor()
        connection_str.append('DRIVER={{{}}}'.format(driver))
        db_host = kwargs.get('db_host', 'localhost')
        if not db_host is None:
            connection_str.append('SERVER={}'.format(db_host))
        if not kwargs.get('db_name', None):
            raise AdapterException(
                'Database not supplied to driver, cannot connect')
        else:
            connection_str.append('DATABASE={}'.format(kwargs.get('db_name')))
        if kwargs.get('trusted', False):
            connection_str.append('Trusted_Connection=yes')
            
        if not kwargs.get('uid') is None:
            connection_str.append(kwargs.get('uid'))
        if not kwargs.get('pwd') is None:
            connection_str.append('PWD=', kwargs.get('pwd'))
            
        # Converts array to string separated by ; characters into configuration
        conn_str = ';'.join(connection_str)
        return conn_str

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
            self.tpreamble = ['USE {};'.format(kwargs.get('db_name')), 
                              'BEGIN TRANSACTION {};'.format(self.tname)]
            self.tobjects = args
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

    