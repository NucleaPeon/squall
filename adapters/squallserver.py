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
    
    
    def disconnect(self):
        pass
    
    def insert(self):
        pass
    
    def update(self):
        pass
        
    def delete(self):
        pass
    
    def select(self):
        pass
    
    def commit(self):
        pass
        
    def rollback(self):
        pass