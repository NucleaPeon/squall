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

import squall

class SqlAdapter():
    '''
    API for calling odbc (sql server)
    
    Expects the odbc module as module parameter
    '''
    
    def __init__(self, module):
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
            
#         if not kwargs.get('uid') is None:
#             connection_str.append(kwargs.get('uid'))
#         if not kwargs.get('pwd') is None:
#             connection_str.append('PWD=', kwargs.get('pwd'))
        # Converts array to string separated by ; characters into configuration
        self.conn = self.module.connect(';'.join(connection_str)) 
        
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self):
        self.conn.close()
    
    def insert(self, sql, params, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any insert specific code is required,
        put it callback.
        '''
        if not precallback is None:
            precallback()
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
    
    def update(self, sql, params, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any update specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback()
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def select(self, sql, params, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any select specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback()
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
        
    def delete(self, sql, params, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any delete specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback()
        self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return self.conn
    
    def sql(self, sql, params):
        
        if "IF EXISTS" in sql:
            print("Warning: SQL Server does not allow IF EXISTS clauses")
            print("-- correct if wrong")
        self.cursor.execute(sql, params)
        return self.conn
    
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        self.conn.rollback()