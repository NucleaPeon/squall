#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

'''
squallsqlite3 is the Squall SqlAdapter class for sqlite3 databases

Basic Overview:
    Do not call the adapter methods directly.
    
    Use squallsqlite3.Insert()
        instead of 
    squallsqlite3.SqlAdapter.insert()
    
    The reason for this is that the adapter's insert() method also controls
    how the sql interacts with the databaser via the connection and driver.
    Using the TitleCase classes that are globally available, you interact
    with dynamic objects that will not cause damage to the database unless
    used in a Transaction object, which is safer and better practice,
    in addition to allowing you to handle exceptions from a centralized
    place.


'''

import sys, os
sys.path.append(os.path.join('..'))

import squallsql

class SqlAdapter(squallsql.Squall):
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
    
    def insert(self, sqlobject, precallback=None, postcallback=None):
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
            
        :Returns:
            - connection object
        '''
        if not precallback is None:
            # Submit parameters as a non-required dictionary
            precallback(**{'method':self.insert, 'class':self, 
                           'sql':sql})
        conn = self.sql(sqlobject)
        if not postcallback is None:
            postcallback(**{'method':self.insert, 'class':self, 
                           'sql':sql})
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
    
    def sql(self, sql):
        self.cursor.execute(sql)
        return self.conn
    
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        raise self.module.IntegrityError()
    
class Insert(squallsql.Sql):
    def __init__(self, table, field, values):
        super().__init__('INSERT', table, field, values)
        self.table = table
        self.field = field
        self.values = values
        
    def __repr__(self):
        return "INSERT INTO {}{} VALUES ({})".format(self.table, 
                                self.fields,
                                ', '.join(str(x) for x in self.values))
        
class Select(squallsql.Sql):
    def __init__(self, table, fields, condition=None):
        super().__init__('SELECT', table, fields, condition)
        self.table = table
        self.fields = fields
        if isinstance(condition, squallsql.Where):
            self.condition = condition
        elif conditions is None: 
            self.condition = ''
        else:
            self.condition = condition
        self.lastqueryresults = ''
        
    def __repr__(self):
        return '''SELECT {} FROM {} {}'''.format( 
             self.fields, self.table, self.condition) 
        
class Verbatim(squallsql.Sql):
    # TODO
    def __init__(self, sql, params):
        self.sql = sql
        self.params = params
        