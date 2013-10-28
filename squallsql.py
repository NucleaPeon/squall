'''
SquallSql is the python module that contains Sql Object references and
generic driver references.
'''
import squall
import importlib
from squallerrors import *

class SqlAdapter(object):
    '''
    :Description:
        Generic SqlAdapter class that calls the specific 
        sql adapter.
        
        All methods that start with an uppercase letter are
        ones that we expect to be called within normal operations
        of the software.
        
        Methods with lowercase starting letters are expected to cause
        strange behaviour or interfere with best practices and should
        be avoided unless the user is testing or knows what they are
        doing.
        
    '''
    
    # - Begin SQL Specific Definitions
    SQL = {
           'Select' : squall.Select,
           'Condition' : squall.Condition,
           'Drop' : squall.Drop,
           'Create' : squall.Create,
           'Union' : squall.Union,
           'Select' : squall.Select,
           'Insert' : squall.Insert,
           'Delete' : squall.Delete,
           'Update' : squall.Update,
           'Where' : squall.Where,
           'WhereIn' : squall.Where,
           'Order' : squall.Order,
           'Exists' : squall.Exists,
           'Value' : squall.Value,
           'Table' : squall.Table,
           'Fields' : squall.Fields,
           'Field' : squall.Field,
           'Group' : squall.Group,
           'Having' : squall.Having,
           'Verbatim' : squall.Verbatim,
           'Key' : squall.Key,
           'PrimaryKey' : squall.PrimaryKey,
           'ForeignKey' : squall.ForeignKey
    }
    # - End SQL Specific Definitions
    
    sqladapter = None
    
    def __init__(self, *args, **kwargs):
        '''
        '''
        import importlib
        # Excepts the filename (without extension) of the adapter module
        self.module = importlib.import_module(kwargs.get('driver'), None)
        if self.module is None:
            raise AdapterException()
        self.sqladapter = self.module.SqlAdapter(*args, **kwargs)
        for sqlclass in self.SQL.keys():
            if hasattr(self.module.SqlAdapter, str(sqlclass)):
                self.SQL[sqlclass] = getattr(self.sqladapter, sqlclass)

    # LIST ADAPTER METHODS
    # Sql Methods can be called on a class.variable basis if class init'd
        
    def Connect(self, *args, **kwargs):
        return self.sqladapter.connect(*args, **kwargs)
    
    def Disconnect(self, *args, **kwargs):
        return self.sqladapter.disconnect(*args, **kwargs)
    
    def sql(self, *args, **kwargs):
        return self.sqladapter.sql(*args, **kwargs)
    
    def Commit(self, *args, **kwargs):
        return self.sqladapter.commit()
    
    def Rollback(self, *args, **kwargs):
        return self.sqladapter.rollback()
    
    #DEPRECATED
    def Verbatim(self, sql, params=()):
        return self.sqladapter.sql_compat(sql, params)
    
    def Transaction(self, *args, **kwargs):
        return self.sqladapter.transaction(*args, **kwargs)
    
    
