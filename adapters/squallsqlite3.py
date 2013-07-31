#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

class SqlAdapter():
    '''
    API for calling sqlite3
    
    Expects the sqlite3 module as module parameter
    '''
    
    def __init__(self, module):
        self.module = module
    
    def connect(self, db_name, **kwargs):
        '''
        :Description:
        
        :Parameters:
            - db_name: string; name of database file
            - **kwargs: dictionary; contains keywords and associated values:
                - db_host: string; hostname -- in sqlite3, only localhost is applicable
                  and all other values will be ignored.
        '''
        db_host = 'localhost'
        if not kwargs.get('db_host') is None:
            db_host = kwargs.get('db_host')
        self.conn = self.module.connect(db_name)
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self, rollback=False):
        if rollback:
            self.rollback()
        else:
            self.commit()
        self.conn.close()
        self.module = None
    
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
        self.cursor.execute(sql, params)
        return self.conn
    
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        raise self.module.IntegrityError()