'''
SquallSql is the python module that contains basic Sql objects that get
converted into SQL syntactical strings yet can be manipulated as an
object. Does not include database specific code.
'''
import squall, sys, os

sys.path.append(os.path.join(os.getcwd(), 'adapters'))

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
    sqladapter = None
    
    def __init__(self, *args, **kwargs):
        '''
        '''
        super().__init__()
        import importlib
        self.module = importlib.import_module(kwargs.get('driver'), '')
        if self.module is None:
            raise squall.AdapterException()
        self.sqladapter = self.module.SqlAdapter(*args, **kwargs)
        
    def Connect(self, *args, **kwargs):
        return self.sqladapter.connect(*args, **kwargs)
    
    def Disconnect(self, *args, **kwargs):
        return self.sqladapter.disconnect(*args, **kwargs)
    
    def Select(self, *args, **kwargs):
        return squall.Select(*args, **kwargs)
        
    def Update(self, *args, **kwargs):
        return squall.Update(*args, **kwargs)
    
    def Delete(self, *args, **kwargs):
        return squall.Delete(*args, **kwargs)
    
    def Insert(self, *args, **kwargs):
        return squall.Insert(*args, **kwargs)
    
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
    
    
