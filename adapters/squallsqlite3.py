#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

import squallsql

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
        
        :Description:
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
            - params: tuple of parameters based on '?' in sql statement
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            # Submit parameters as a non-required dictionary
            precallback(**{'method':self.insert, 'class':self, 
                           'sql':sql, 'params':params})
        conn = self.sql(sql, params)
        if not postcallback is None:
            postcallback(**{'method':self.insert, 'class':self, 
                           'sql':sql, 'params':params})
        return conn
    
    def update(self, sql, params, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any update specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback()
        conn = self.sql(sql, params)
        if not postcallback is None:
            postcallback()
        return conn        
    
    def select(self, selectobject, precallback=None, postcallback=None):
        '''
        '''
        if not precallback is None:
            precallback()
        self.sql(str(selectobject))
        selectobject.lastqueryresults = self.cursor.fetchall() 
        if not postcallback is None:
            postcallback()
        return selectobject.lastqueryresults
        
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
    
    def sqldate(self):
        return self.select('''SELECT date('now');''', ())
    
    def sql(self, sql, params):
        self.cursor.execute(sql, params)
        return self.conn
    
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        raise self.module.IntegrityError()
    
class Insert(squallsql.Sql):
    def __init__(self, table, fields, values):
        super().__init__('INSERT', table, fields, values)
        self.table = table
        self.fields = fields
        self.values = values
        
    def __repr__(self):
        if len(self.fields) > 0:
            self.fields = "{}{}{}".format("(", ', '.join(self.fields), ")")
        return "INSERT INTO {} {} VALUES ({})".format(self.table, 
                                self.fields,
                                ', '.join(str(x) for x in self.values))
        
class Select(squallsql.Sql):
    def __init__(self, table, fields, conditions=[]):
        super().__init__('SELECT', table, fields, conditions)
        self.table = table
        self.fields = fields
        self.conditions = conditions
        self.lastqueryresults = ''
        
    def __repr__(self):
        return '''{} INTO {} VALUES {}'''.format(self.command, 
            self.fields, self.values)