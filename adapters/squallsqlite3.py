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
        # Return connection object
        pass
    
    def insert(self, success_callback=None, failure_callback=None):
        pass
    
    def select(self, success_callback=None, failure_callback=None):
        pass
    
    def update(self, success_callback=None, failure_callback=None):
        pass
    
    def delete(self, success_callback=None, failure_callback=None):
        pass