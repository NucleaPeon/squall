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

import squallsql, squall # for exceptions

class SqlAdapter(squallsql.Squall):
    '''
    API for calling sqlite3
    
    Expects the sqlite3 module as module parameter
    '''
    
    def __init__(self, module):
        super().__init__()
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
        self.db_name = db_name
        if not kwargs.get('db_host') is None:
            db_host = kwargs.get('db_host')
        self.conn = self.module.connect(self.db_name)
        self.cursor = self.conn.cursor() # We need this cursor in the class
        return self.conn
    
    def disconnect(self):
        '''
        :Description:
            Disconnect the driver from the database.
        '''
        self.conn.close()
    
    def insert(self, sqlobject, precallback=None, postcallback=None):
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
            # Submit parameters as a non-required dictionary
            precallback(**{'method':self.insert, 'class':self, 
                           'sql':sqlobject})
        conn = self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback(**{'method':self.insert, 'class':self, 
                           'sql':sqlobject})
        return self.conn
    
    def update(self, sql, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any update specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback(**{'method':self.update, 'class':self,
                           'sql':sql})
        self.sql(str(sql))
        if not postcallback is None:
            postcallback(**{'method':self.update, 'class':self,
                           'sql':sql})
        return self.conn        
    
    def select(self, sql, precallback=None, postcallback=None):
        '''
        :Description:
            In order to get the sql results in the postcallback, look for the
            'result' **kwargs in the postcallback method. You will need to set
            the method parameter with **kwargs in order to do so. 
            
        :Returns:
            - list: sql results of query in a tuple [(row1 data), (rowN data)]
                - rowN data represents the fields selected
        '''
        if not precallback is None:
            precallback(**{'method':self.update, 'class':self,
                           'sql':sql})
        self.sql(str(sql)) 
        results = self.cursor.fetchall()
        if not postcallback is None:
            postcallback(**{'method':self.update, 'class':self,
                           'sql':sql, 'result':results})
        return results
        
    def delete(self, sqlobject, precallback=None, postcallback=None):
        '''
        Go directly to sql(), if any delete specific code is required,
        put it here.
        '''
        if not precallback is None:
            precallback()
        self.sql(str(sqlobject))
        if not postcallback is None:
            postcallback()
        return self.conn
    
    def sql(self, sql, param=()):
        self.cursor.execute(str(sql), param)
        return self.conn
    
    def sql_compat(self, sql, param=()):
        '''
        Compatibility (temporary) sql method to force return of rows in 
        the execute call. 
        '''
        self.cursor.execute(str(sql, param))
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
        raise self.module.IntegrityError()
    
class Insert(squallsql.Sql):
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
        
class Select(squallsql.Sql):
    def __init__(self, table, fields, condition=None, precallback=None,
                 postcallback=None):
        '''
        :Description:
        :Parameters:
            - precallback: method; passed to the Transaction object which passes
              it to the sqlobj (adapter) .select() statement
            - postcallback: method; passed to the Transaction object which passes
              it to the sqlobj (adapter) .select() statement. Adding a method 
              here which garners the kwargs.get('result') call will fetch 
              results of the statement.  
        '''
        super().__init__('SELECT', table, fields, condition, precallback,
                         postcallback=None)
        self.table = table
        self.fields = fields
        self.existsflag = False
        if isinstance(condition, squallsql.Where):
            self.condition = condition
        elif isinstance(condition, squallsql.Exists):
            # FIXME: Bad coding practice, may get rid of
            self.existsflag = True
        else: 
            self.condition = ''
        self.lastqueryresults = ''
        
    def __repr__(self):
        if self.existsflag:
            return '''SELECT EXISTS({} FROM {} {})'''.format( 
             self.fields, self.table)
            
        return '''SELECT {} FROM {} {}'''.format( 
             self.fields, self.table, self.condition) 
        
class Delete(squallsql.Sql):
    def __init__(self, table, condition=None):
        super().__init__('DELETE', table=table, condition=condition)
        self.table = table
        if isinstance(condition, squallsql.Where):
            self.condition = condition
        else:
            self.condition = ''
        
    def __repr__(self):
        return "DELETE FROM {} {}".format(self.table, self.condition)
        
class Update(squallsql.Sql):
    def __init__(self, table, field, values, condition=None):
        super().__init__('UPDATE', table=table, field=field, values=values,
                         condition=condition)
        self.table = table
        self.field = field
        self.values = values
        if condition is None:
            self.condition = ''
        else:
            self.condition = condition
        
    def __parse_values(self, field, value):
        return "{} = {}".format(field, value)
        
    def __repr__(self):
        cond = ''
        if not self.condition is None:
            cond = ' {}'.format(str(self.condition))
        params = []
        
        if len(self.field.fields) == 1:
            # Not an array, but one field
            if not isinstance(self.values, str):
                raise squall.InvalidSqlValueException(
                    'Non-Equal fields [1] to values [{}] ratio'.format(
                        len(self.values)))
            return "UPDATE {} SET {} = {}{}".format(self.table, ', '.join(params), cond)
        for i in range(0, len(self.values)):
            params.append(self.__parse_values(self.field.fields[i], self.values[i]))
        
        return "UPDATE {} SET {}{}".format(self.table, ', '.join(params), cond)
        
class Transaction(squallsql.Transaction):
    '''
    Sqlite3 Transaction object
    
    :Description:
        Manages transactions from an sqlite3-specific perspective.
        Because sqlite3 already includes transaction functionality,
        we can utilize the base object in squallsql.
        
        Rollbacks raise an exception.
    '''
    
    def __init__(self, adapter, *args):
        super().__init__(adapter, *args)
        
        
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
        

        