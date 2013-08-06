#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
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

import squall, squallsql

class SqlAdapter(squallsql.Squall):
    '''
    API for calling odbc (sql server)
    
    Expects the odbc module as module parameter
    '''
    
    def __init__(self, module):
        super().__init__()
        self.module = module
        
    def connect(self, db_name, **kwargs):
        '''
        :Parameters:
            - db_name: string; name of database file
            - **kwargs: dictionary; contains list of parameters for connections
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
        driver = kwargs.get('driver', 'SQL Server')
        connection_str.append('DRIVER={{{}}}'.format(driver))
        db_host = kwargs.get('db_host', 'localhost')
        if not db_host is None:
            connection_str.append('SERVER={}'.format(db_host))
        if not db_name:
            raise squall.AdapterException(
                'Database not supplied to driver, cannot connect')
        else:
            connection_str.append('DATABASE={}'.format(db_name))
        if kwargs.get('trusted', False):
            connection_str.append('Trusted_Connection=yes')
            
        if not kwargs.get('uid') is None:
            connection_str.append(kwargs.get('uid'))
        if not kwargs.get('pwd') is None:
            connection_str.append('PWD=', kwargs.get('pwd'))
            
        # Converts array to string separated by ; characters into configuration
        conn_str = ';'.join(connection_str)
        self.conn = self.module.connect(conn_str) 
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self):
        '''
        :Description:
            Disconnect the driver from the database.
        '''
        self.conn.close()
    
    def insert(self, sql, params, precallback=None, postcallback=None):
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
    
    def update(self, sql, params, precallback=None, postcallback=None):
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
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def select(self, sql, params, precallback=None, postcallback=None):
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
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def delete(self, sql, params, precallback=None, postcallback=None):
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
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
    
    def sql(self, sql):
        '''
        :Description:
            Executes the sql string
            
        :Parameters:
            - sql: string; sql statement
        '''
        self.cursor.execute(sql)
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
        
class Insert(squallsql.Sql):
    '''
    Insert object that inherits Base Insert object in
    squallsql. 
    
    Note: Same as sqlite3 driver, which this is based off of.
    
    :Description:
        Sql Server Insert object that properly formats insert statements
        in sql server format.
        
    :Parameters:
        - table: Table() object; 
        - field: Fields() object;
        - values: [Value()] object;
    '''
    def __init__(self, table, field, values):
        super().__init__('INSERT', table, field, values)
        self.table = table
        self.field = field
        self.values = values
        
    def __repr__(self):
        mf = self.field
        if self.field.fields != '':
            mf = '{}{}{}'.format(' (', mf, ')')
        return "INSERT INTO {}{} VALUES ({})".format(self.table, 
                                mf,
                                ', '.join(str(x) for x in self.values))
        
class Verbatim(squallsql.Sql):
    '''
    :Description:
        Verbatim is a class whose purpose is to pipe direct
        string sql commands into the database driver. This is to
        allow customization by preference of the developer.
    '''
    # TODO
    def __init__(self, sql):
        self.sql = sql
        
    def __repr__(self):
        return "{}".format(self.sql)
    