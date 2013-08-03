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

class Sql():
    '''
    Sql Meta Object
    
    Transaction functionality such as rolling back and committing
    is handled by the driver adapters.
    (Queue list of sql objects or strings)
    '''
    
    COMMANDS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP']
    
    def __init__(self, command, table, fields = ['*'], 
                 values = [], conditions = []):
        '''
        :Conditions:
        '''
        
        self.command = Command(command.upper())
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
        #FIXME: Problem with this command is that it doesn't take into account
        # the "FROM" portion of queries and those which don't (UPDATE, INSERT)
        # Need to differentiate from "FROM", "SET", and "VALUES"
        return "{} {} {} {} {}".format(self.command, self.fields, self.table,
                                       self.values, self.conditions)
class Command():
    
    def __init__(self, command):
        self.command = command
        if not self.command in Sql.COMMANDS:
            raise squall.InvalidSqlCommandException(
                'Command {} is not a valid command to issue'.format(
                    str(command)))
        
    def __repr__(self):
        return self.command
        
class Condition():
    def __init__(self):
        pass
    
    def __repr__(self):
        return ''
        
class Where(Condition):
    
    def __init__(self, field, operator, value, conditions=[]):
        '''
        TODO
        :Parameters:
            - value: string; can be an Sql object IF AND ONLY IF
              command == 'SELECT', otherwise expects a string
              representation
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
        self.field = field
        self.operator = operator
        if type(value) == Command:
            if not value.command == "SELECT":
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
        return " WHERE {} {} {} {}".format(self.field, self.operator,
                                          self.value, self.conditions)
        
class Exists(Condition):
    
    def __init__(self, exists=True):
        self.exists = exists
        
    def __repr__(self):
        if self.exists:
            return "IF EXISTS"
        else:
            return "IF NOT EXISTS"
    
class Values():
    def __init__(self, values):
        if not type(values) in [list, tuple]:
            raise squall.InvalidSqlValueException(
                'Values must be in tuple or list format') 
        self.values = values
        
    def __repr__(self):
        return ', '.join(values)
    
class Table():
    
    def __init__(self, table):
        self.table == table
        
    def __repr__(self):
        return table
    
class Fields():
    
    def __init__(self, *args):
        # A Wildcard eliminates the need for any additional fields
        if '*' in args:
            args = '*'
        if isinstance(list, args):
            args = ', '.join(args) # Convert to string
        self.fields = args
        
    def __repr__(self):
        return ', '.join(self.fields)