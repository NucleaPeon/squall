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
    
    def connect(self, db_name, db_host='localhost'):
        '''
        :Parameters:
            - db_name: string; name of database file
            - db_host: string; not used
        '''
        conn = self.module.connect(db_name)
        self.cursor = conn.cursor() # We need this cursor in the class
        return conn
    
    def disconnect(self, rollback=False):
        if rollback:
            self.rollback()
        else:
            self.commit()
        self.module.close()
        self.module = None
    
    def commit(self):
        self.cursor.commit()
        
    def rollback(self):
        raise self.module.IntegrityError()