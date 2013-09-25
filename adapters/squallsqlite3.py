#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#
# FIXME: _instance to SqlAdapter(), remove kwargs dependency on adapter keyword.
#        add kwargs for all parameters. table=Table(), fields=Field(), etc.
'''
squallsqlite3 is the Squall SqlAdapter class for sqlite3 databases

Basic Overview:
    This python module contains sqlite3 specific code that is accessed through
    the generic SqlAdapter in squallsql.
    
    It is recommended that the user use Transaction objects (also found in
    squallsql) instead of direct adapter sql/sql_compat methods
'''

import sys, squall, sqlite3

class SqlAdapter(object):
    '''
    :Description:
        API for calling sqlite3 database
    '''
    conn = None
    cursor = None
    
    _instance = None
    def __new__(self, *args, **kwargs):
        if not self._instance:
            self._instance = super().__new__(self)
        return self._instance
        
    
    def connect(self, *args, **kwargs):
        '''
        :Description:
        
        :Parameters:
            - **kwargs: dictionary; contains keywords and associated values:
                - host: string; hostname -- in sqlite3, only localhost is applicable
                  and all other values will be ignored.
                - database: string; location of database file
        '''
        if not self.conn is None:
            return self.conn
        db_host = 'localhost'
        self.db_name = kwargs.get('database', None)
        if self.db_name is None:
            raise squall.InvalidDatabaseNameException(
                'Did not find database name parameter with SqlAdapter init')
        if not kwargs.get('host') is None:
            db_host = kwargs.get('host')
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self):
        '''
        :Description:
            Disconnect the driver from the database.
        '''
        self.conn.close()
        self.conn = None
    
    def sql(self, sqlobject):
        self.cursor.execute(str(sqlobject))
        return self.conn
    
    def sql_compat(self, sql, param=()):
        '''
        Compatibility (temporary) sql method to force return of rows in 
        the execute call. 
        No transaction capabilities in this method.
        '''
        self.cursor.execute(sql, param)
        return self.cursor.fetchall()
    
    def commit(self):
        '''
        Deprecated in favour of Transaction objects
        :Description:
            Calls the commit() function of the database connection
            driver.
            
        :See:
            Sqlite3 already does some commit and rollback functionality on its own, without
            our help.
            - http://docs.python.org/2/library/sqlite3.html#sqlite3-controlling-transactions
        '''
        self.conn.commit()
        
    def rollback(self):
        '''
        :Description:
            Explicitly invoke a rollback exception for sqlite3
        '''
        raise squall.RollbackException('rollback() method invoked')
    

        
    class Transaction(squall.Sql):
        '''
        :Description:
            Transaction object that takes a list of Squall Command objects and will
            commit() or rollback() based on whether one failure is detected.
            
            This object contains two main methods: run() and pretend()
            pretend() imitates run() but regardless of options, will not run commit()
            run() will attempt to commit unless an exception is raised.
            See run() method for parameter listings
            
            It is recommended that one overrides this class in their database
            driver/adapter class so they can integrate better with their own objects
            and make use of callbacks/sql exceptions
            
        :Parameters:
            - **kwargs: dict;
                - adapter: object; committing and rolling back statements hinges on
                  this object. Requires commit() and rollback(). Raises a 
                  MissingDatabaseAdapterException if None is supplied.
                  Defaults to the instance of the driver class this method is 
                  implemented in.
                - precallback: method; during run() method, this will get called 
                  before commit or rollback statement.
                  TODO: list params method can use
                - postcallback: method; during run() method, this will get called
                  after commit or rollback statement.
                  TODO: list params method can use
        '''
        def __init__(self, *args, **kwargs):
            self.tobjects = []
            self.output = {}
            self.add(*args)
            self.adapter = kwargs.get('adapter', SqlAdapter._instance)
            if self.adapter is None:
                raise squall.MissingDatabaseAdapterException('No adapter object to connect to')
            
        def add(self, *args):
            '''
            :Returns:
                - args: list; all sqlobjects that were provided as arguments
            '''
            for a in args:
                if not isinstance(a, squall.Sql):
                    if isinstance(a, str):
                        self.tobjects.append(squall.Verbatim(a))
                        continue
                    else:
                        raise squall.InvalidSquallObjectException(
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
                raise squall.EmptyTransactionException('No objects to execute')
            for tobj in self.tobjects:
                if not isinstance(tobj, squall.Sql):
                    raise squall.InvalidSquallObjectException('{} is invalid'.format(
                        str(tobj)))
                
            for squallobj in self.tobjects:
                if isinstance(squallobj, squall.Select):
                    self.output[str(squallobj)] = self.adapter.sql_compat(str(squallobj))
                else:
                    self.adapter.sql(str(squallobj)) # This will raise a rollback exception 
                # via sqlite3, so we don't have to check for this. Other db's will have
                # to reimplement this.
            self.adapter.commit()
            
            if not kwargs.get('raise_exception') is None:
                raise squall.CommitException('Committed Transaction')
            return self.clear()
                
        def pretend(self):
            if len(self.tobjects) == 0:
                raise squall.EmptyTransactionException('No objects to execute')
            for tobj in self.tobjects:
                if not isinstance(tobj, squall.Sql):
                    raise squall.InvalidSquallObjectException('{} is invalid'.format(
                        str(tobj)))
                    
            try:
                for squallobj in self.tobjects:
                    self.adapter.sql(str(squallobj))
                self.adapter.rollback()
            except Exception:
                raise squall.RollbackException(
                    'Exception raised: {}'.format(sys.exc_info()[0]))
            return self.tobjects
        
        def __repr__(self):
            return '\n'.join(str(x) for x in self.tobjects)
        
