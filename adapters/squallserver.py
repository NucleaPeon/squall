#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

class SqlAdapter():
    '''
    API for calling odbc (sql server)
    
    Expects the odbc module as module parameter
    '''
    
    def __init__(self, module):
        self.module = module
        
    def connect(self, db_name, uid, pw, db_host='localhost'):
        '''
        :Parameters:
            - db_name: string; name of database file
            - db_host: string; not used
        '''
        self.conn = connect(driver='{SQL Server}', server=db_host, database=db_name,
                            uid=uid, pwd=pw) # FIXME
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self):
        pass
    
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
    
    def commit(self):
        pass
        
    def rollback(self):
        pass