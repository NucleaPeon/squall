'''
Created on Sep 27, 2013

:Description:
    Module that contains squall exceptions

@author: Daniel Kettle
'''

class AdapterException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        
class MissingDatabaseAdapterException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidSqlCommandException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidSqlValueException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidSqlWhereClauseException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidSqlConditionException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class EmptyTransactionException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)

class RollbackException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class CommitException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidSquallObjectException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidDatabaseNameException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class InvalidDistinctFieldFormat(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)
        
class NotImplementedException(AdapterException):
    def __init__(self, message):
        AdapterException.__init__(self, message)