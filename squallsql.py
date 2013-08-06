'''
SquallSql is the python module that contains the Sql object that gets
converted into SQL syntactical strings yet can be manipulated as an
object.

This is the base parent class and should not be instantiated as a rule.
Most Databases have slightly varying syntaxes so this class cannot
apply to all databases due to those differences.

The database adapter should inherit this class and override the methods
that have functionality that differ.

This base class will return ` what is submitted, except lists
will be parsed with commas

If you want to use this class to produce sql strings, do it like this
in your database adapter class:

:Code:
    import squallsql

    class Sql(squallsql.Sql):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
        
    
'''



import squall, sys

class Squall():
    '''
    :Description:
        Squall is a class object that all other squall-based objects inherit
        so applications can differentiate between classes of the same name
        (if applicable) as many of these classes have generic simple names.
        
        There is no functionality difference in this class as of its
        first version. Future versions may expand on this class's
        functionality.
    '''
    def __init__(self):
        pass

class Sql(Squall):
    '''
    :Description:
        Sql is a GENERIC class for handling sql; this means that its
        parameters are meant to cover all possible sql commands and
        then each specific command, such as SELECT, will inherit this
        class and then only use what it needs.
        
        An Sqlite3 Select object would inherit squallsql.Sql and
        the __init__ function would take table, fields and conditions
        parameters. It would fill in command to SELECT by itself and
        ignore values since that is an INSERT specific parameter.
        
        Truly, this class's purpose is more for managing acceptable
        values that all other objects will build off of, a sanity
        check more than an actually used class object.
    
    :Parameters:
        - command: Command object; use the following strings to initialize object
          successfully. NOTE: strings are NOT case sensitive for Command(), but ARE
          for other components, so only use Command() objects 
            - SELECT
            - INSERT
            - UPDATE
            - DELETE
            - CREATE
            - DROP
          
            
          Any more commands that sql uses can be added to the Sql.COMMANDS
          list, otherwise an InvalidSqlConditionException is raised.
          
        - table: string; name of the table to act on. 
    '''
    
    '''
    :Class Variables:
        - COMMANDS: list; values of acceptable commands. See Class :Parameters:
          section or print(str(Sql(...).COMMANDS)) 
    '''
    COMMANDS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP']
    
    def __init__(self, command='', table=None, field = None, 
                 values = [], condition = None):
        
        super().__init__()
        self.command = Command(command)
        if not str(self.command) in Sql.COMMANDS:
            raise squall.InvalidSqlCommandException(
                'Command {} is not a valid command to issue'.format(
                    str(command)))
            
        self.table = table
        self.fields = field
        self.values = values
        if condition is None:
            self.condition = ''
        elif not isinstance(condition, Condition):
            raise squall.InvalidSqlConditionException(
                '{} are not Condition objects'.format(
                    str(condition)))
        else:
            self.condition = condition
            
    def __repr__(self):
        '''
        :Description:
            Represents the Sql() object by returning all class parameters
            as a string -- which will inevitably fail if actually used to
            call sql. It only represents the base object for the purpose
            of introspection.
            
        :Returns:
            - string: all parameters in one string separated by spaces.
              Do not try to split by spaces, as it will not work since some
              parameters are str(list) conversions or object->string conversions.
        '''
        #FIXME: Problem with this command is that it doesn't take into account
        # the "FROM" portion of queries and those which don't (UPDATE, INSERT)
        # Need to differentiate from "FROM", "SET", and "VALUES"
        return "{} {} {} {} {}".format(self.command, str(self.field), self.table,
                                       str(self.values), str(self.condition))

class Command(Squall):
    def __init__(self, command):
        super().__init__()
        self.command = str(command).upper()
        
    def __repr__(self):
        return self.command
            
class Condition(Squall):
    def __init__(self, conditions = []):
        super().__init__()
        
class Where(Condition):
    
    def __init__(self, field, operator, value, conditions=[]):
        '''
        :Description:
            This is the main condition that gets used.
            It follows the struture (usually) of:
                - WHERE field [operator] value [optional conditions]
                    - operator: string/character; =, >=, <=, IN, etc. etc.
                    - optional conditions: list of Condition objects
                        - Where(), Exists(), Where(..., Select())
                      Conditions can specify more conditions to give sql output like this:
                      
                      SELECT * FROM table WHERE pk = 'value' AND fk = 'secondary'
                      
                      You write it like this: 
                      Select('table', ['*'], Where('pk', '=', 'value', Where(
                          'fk', '=', 'secondary'))
                          
                      Subqueries require that a Select object be added to the conditions
                      list:
                      
                      Where('pk', '=', 'value', Where('value', 'IN', 
                          Select('tbl', ['values'], Where('COUNT(values)', '>=', 5))))
                          
                      It gets complicated, but allows for easier and dynamic code generation 
                      of sql over time and multiple databases.
            
        :Parameters:
            - value: string; can be an Sql object IF AND ONLY IF
              command == 'SELECT', otherwise expects a string
              representation. 
            - conditions: tuple; Either: 
                - A list of strings, which are joined to create one string
                - A list of Where or Select objects, where the representation of each
                  additional object is prepended with an "AND" keyword
                (Where('x', '=', '5'), Where('y', '=', '7')) would equal:
                WHERE x = 5 AND y = 7
                If a substring was placed into the condition, it could look like this:
                (Where('x', '=', '5'), Select('t', 'y', Where('y', '=', '7')))
                >> WHERE x = 5 AND (SELECT y FROM t WHERE y = 7)
                
        '''
        super().__init__()
        self.field = field
        if not isinstance(operator, str):
            raise squall.InvalidSqlConditionException('Operator not valid: {}'.format(operator))
        self.operator = operator
        if type(value) == Sql or type(value) == Value:
            if type(value) == Value:
                self.value = str(value)
            elif not value.command == "SELECT":
                raise squall.InvalidSqlWhereClauseException(
                    'Non-Queries not allowed in WHERE Clause') 
            else:
                self.value = "({})".format(str(value))
        elif isinstance(value, str):
            self.value = value
        else:
            raise squall.InvalidSqlWhereClauseException(
                        'Invalid WHERE Value {}'.format(value))
        
        if len(conditions) > 0:
            if isinstance(conditions[0], str):
                self.conditions = ' '.join(conditions)
            else:
                if len(conditions) > 0:  
                    self.conditions = ' '.join(
                                str(cond) for cond in conditions).replace("WHERE", "AND")
                else:
                    self.conditions = '' 
        else:
            self.conditions = ''
        
    def __repr__(self):
        return "WHERE {} {} {} {}".format(self.field, self.operator,
                                          self.value, self.conditions)
        
class Exists(Condition):
    '''
    :Description:
        Appends the IF EXISTS or IF NOT EXISTS sql condition when handling 
        tables or columns.
    '''
    
    def __init__(self, exists=True, conditions = []):
        super().__init__()
        self.exists = exists
        if len(conditions) > 0:
            if isinstance(conditions[0], str): # String list:
                self.conditions = ' '.join(conditions)
            else:
                if len(conditions) > 0:  
                    self.conditions = ' '.join(
                                str(cond) for cond in conditions).replace("IF EXISTS", "AND")
                    self.conditions.replce('IF NOT EXISTS', "AND")
                else:
                    self.conditions = '' 
        else:
            self.conditions = ''
        
    def __repr__(self):
        if self.exists:
            return "IF EXISTS"
        else:
            return "IF NOT EXISTS"
    
class Value(Squall):
    '''
    :Description:
        Value() is a way to ensure the database types are met.
        I am making an executive decision to force database types
        into native python types and vice versa.
        Therefore, if you make a Select command using squall, in the
        results it will return a pythonic date object (datetime module)
        instead of a string. Likewise, when calling an Insert() object,
        the value object should be a datetime object, which will get
        converted to the appropriate string or methodcall by the database
        adapter.
        
        For instance: if type(value) == str in python,
            output: \'\'\' 'value' \'\'\' (quoted)
        
        if type(value) == datetime in python, 
            output: \'\'\'strftime('%Y-%m-%d %H:%M:%S', '2004-01-01 02:34:56')\'\'\'
        
        where '2004-01-01 02:34:56' is the output of a str(datetime) object type
    '''
    def __init__(self, value): 
        self.value = value
        self.type = type(value)
        
    def __repr__(self):
        if self.type == int:
            return str(self.value)
        elif self.type == str:
            return "'{}'".format(str(self.value))
        return "{}: {}".format(self.value, self.type)
    
class Table(Squall):
    '''
    :Description:
        A class that represents a table name.
    '''
    def __init__(self, table):
        self.table = table
        
    def __repr__(self):
        return str(self.table)
    
class Fields(Squall):
    '''
    :Description:
        Class representation of field lists in a class for more
        functionality. Represented by a comma-delimited string
        of values
    '''
    
    def __init__(self, *args):
        # A Wildcard eliminates the need for any additional fields
        if len(args) == 0:
            args = '' # Empty, so INSERT statements don't fail, need empty string
        elif '*' in args:
            args = ['*']
        else:
            if not type(args) in [list, tuple]:
                raise InvalidSqlValueException(
                        'Value is neither a wildcard char nor a list or tuple')
        self.fields = args
                
    def __repr__(self):
        return ', '.join(self.fields)
    
class Transaction(Squall):
    '''
    :Description:
        Transaction object that takes a list of Squall Command objects and will
        commit() or rollback() based on whether one failure is detected.
        
        This object contains two main methods: run() and pretend()
        pretend() imitates run() but regardless of options, will not run commit()
        run() will attempt to commit unless an exception is raised.
        See run() method for parameter listings
        
        It is recommended that one overrides this class in their database
        driver/adapter class so they can integrate better with their own objects
        and make use of callbacks/sql exceptions
        
        
    '''
    def __init__(self, adapter, *args):
        self.tobjects = []
        for a in args:
            self.add(a) # Will raise exception if invalid object found
        self.adapter = adapter
        
    def add(self, *args):
        for a in args:
            if not isinstance(a, Sql):
                raise squall.InvalidSquallObjectException('Cannot add non-sql object {}'.format(
                    str(a)))
            self.tobjects.append(a)
            
    def clear(self):
        '''
        :Returns:
            All the transaction's current sql objects in a list before it clears
            to an empty list.
        '''
        retval = self.tobjects
        self.tobjects = []
        return retval
        
    def run(self, rollback_callback=None, success_callback=None,
            raise_exception=False):
        '''
        :Description:
            Goes through every Squall Command object and runs the sql through
            the adapter object supplied. (When an adapter class overrides this
            method, it should supply the adapter automatically, so only args
            need to be supplied.)
            Raises an exception and attempts to rollback when an exception
            is found, meaning an error arose during sql execution.
            commit() is called if no errors during execution occur.
            
        :Parameters:
            - rollback_callback: If a rollback is called, call this method
            - success_callback: If Transaction completes as expected, call this
              method
            - raise_exception: boolean; raise exceptions on execution completion
              or error. (great for stricter environments) This does mean that
              no return statements will be called unless embedded into the error
              message or object. 
                        
        :Exceptions:
            - EmptyTransactionException: Called when *args is empty and nothing
              can be run
            - RollbackException: when raise_exception is True, committing and
              rolling back will raise an exception. This is raised when a 
              rollback is encountered.
            - CommitException: when raise_exception is True, committing and
              rolling back will raise an exception. This is raised when a 
              commit is successful.
            - InvalidSquallObjectException - If at any point an AdapterException
              is raised during execution of sql objects.
              
        :Returns:
            - None if rollback occured and transaction failed,
            - list if successful commit, list contains all transaction objects
        '''
        if len(self.tobjects) == 0:
            raise squall.EmptyTransactionException('No objects to execute')
        for tobj in self.tobjects:
            if not isinstance(tobj, Sql):
                raise squall.InvalidSquallObjectExecption('{} is invalid'.format(
                    str(tobj)))
            
        for squallobj in self.tobjects:
            self.adapter.sql(str(squallobj)) # This will raise a rollback exception 
            # via sqlite3, so we don't have to check for this. Other db's will have
            # to reimplement this.
        self.adapter.commit()
        if raise_exception:
            raise squall.CommitException('Committed Transaction')
        return self.clear()
            
    def pretend(self):
        if len(self.tobjects) == 0:
            raise squall.EmptyTransactionException('No objects to execute')
        for tobj in self.tobjects:
            if not isinstance(tobj, Sql):
                raise squall.InvalidSquallObjectExecption('{} is invalid'.format(
                    str(tobj)))
                
        try:
            for squallobj in self.tobjects:
                self.adapter.sql(str(squallobj))
            self.adapter.rollback()
        except Exception as E:
            raise squall.RollbackException(
                'Exception raised: {}'.format(sys.exc_info()[0]))
        return self.tobjects
    
    def __repr__(self):
        return '\n'.join(str(x) for x in self.tobjects)
    
class Verbatim(Sql):
    '''
    :Description:
        Verbatim is a class whose purpose is to pipe direct
        string sql commands into the database driver. This is to
        allow customization by preference of the developer.
        
        If params are a tuple that has a lenth > 0, this class checks
        the sql for ? characters and replaces each ? with the parameter
        based on order: first ? == first parameter (params[0])
        
        If more or fewer ?'s exist than params has in length, 
        an error is raised. 
    '''
    # TODO
    def __init__(self, sql):
        self.sql = sql
        
    def __repr__(self):
        return "{}".format(self.sql)