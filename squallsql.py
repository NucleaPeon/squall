'''
SquallSql is the python module that contains the Sql object that gets
converted into SQL syntactical strings yet can be manipulated as an
object.

This is the base parent class and should not be instantiated as a rule.
Most Databases have slightly varying syntaxes so this class cannot
apply to all databases due to those differences.

The database adapter should inherit this class and override the methods
that have functionality that differ.

This base class will return verbatim what is submitted, except lists
will be parsed with commas

If you want to use this class to produce sql strings, do it like this
in your database adapter class:

:Code:
    import squallsql

    class Sql(squallsql.Sql):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
        
    
'''



import squall

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
    
    def __init__(self, command='', table=None, fields = None, 
                 values = [], conditions = []):
        
        super().__init__()
        self.command = Command(command)
        if not str(self.command) in Sql.COMMANDS:
            raise squall.InvalidSqlCommandException(
                'Command {} is not a valid command to issue'.format(
                    str(command)))
            
        self.table = table
        self.fields = fields
        self.values = values
        self.conditions = conditions
        for c in conditions:
            if not isinstance(c, Condition):
                raise squall.InvalidSqlConditionException(
                    '{} are not Condition objects'.format(
                        str(conditions)))
        
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
        return "{} {} {} {} {}".format(self.command, str(self.fields), self.table,
                                       str(self.values), str(self.conditions))

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
    
    def __init__(self, field, operator='', value='', conditions=[]):
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
            args = [''] # Empty, such as in INSERT statements without fields
        elif '*' in args:
            args = ['*']
        elif isinstance(list, args):
            args = ', '.join(args) # Cosvert to string
        elif isintance(tuple, args):
            args = ', '.join(args)
        else:
            raise InvalidSqlValueException(
                    'Value is neither a wildcard char nor a list or tuple')
        self.fields = args
        
    def __repr__(self):
        if type(self.fields) == str:
            return self.fields
        return ', '.join(self.fields)
    
class Transaction(Squall):
    '''
    :Description:
        Transaction object that takes a list of Squall objects and will
        commit() or rollback() based on whether one failure is detected,
        or all objects / strings run without error.
        
    '''
    def __init__(self, *args):
        self.tobjects = args
        
    def __repr__(self):
        return ', '.join(self.tobjects)