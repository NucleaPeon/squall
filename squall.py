#!/usr/bin/env python
#
# Author: Daniel Kettle
# Date:   July 25 2013
#
from collections.abc import Iterable

__all__ = ['Sql', 'Drop', 'Create', 'Select', 'Insert', 'Update', 'Delete', 'Condition',
           'Where', 'WhereIn', 'Having', 'Exists', 'Order',
           'Table', 'Fields', 'Value', 'Group', 'Verbatim']

# Only import what we need
import datetime as dt
from squallerrors import InvalidSqlCommandException, InvalidSqlConditionException, \
                         InvalidSqlWhereClauseException, InvalidSqlValueException, \
                         InvalidDistinctFieldFormat


ADAPTERS = {'sqlite3' : None,
            'sqlserver': None, # Uses odbc drivers
            'mysql': None,
            'postgres': None,
            'firebird': None}


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
    
    def __init__(self, command='', table=None, field=None, 
                 values=[], *args, **kwargs):
        super().__init__()
        self.command = Command(command)
        self.precallback = kwargs.get('precallback')
        self.postcallback = kwargs.get('postcallback')
        if not str(self.command) in self.COMMANDS or \
           self.command == '':
            raise InvalidSqlCommandException(
                'Command <{}> is not a valid command to issue'.format(
                    str(command)))
            
        self.table = kwargs.get('table', None)
        self.fields = kwargs.get('field', None)
        self.values = kwargs.get('values', [])
        self.condition = kwargs.get('condition', None)
        if not self.condition is None:
            if not isinstance(self.condition, Condition):
                raise InvalidSqlConditionException(
                    '{} are not Condition objects'.format(
                        str(self.condition)))
            
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
    
    def __init__(self, field, operator, value):
        super().__init__()
        self.field = field
        self.operator = operator
        if isinstance(value, Sql):
            if isinstance(value, Select):
                self.value = "({})".format(str(value))
            elif "SqlAdapter.Value" in str(type(value)):
                self.value = str(value)
            else:
                self.value = value
            
        elif isinstance(value, str):
            self.value = value
        else:
            raise InvalidSqlWhereClauseException(
                        'Invalid Condition Value {}'.format(value))
        
    def __eq__(self, other):
        return str(self) == str(other)
        
    def __repr__(self):
        return "{} {} {}".format(self.field,
                                 self.operator,
                                 self.value).strip()

class Drop(Sql):
    
    def __init__(self, table, exists=None, **kwargs):
        super().__init__('DROP', table, exists, **kwargs)
        self.table = table
        self.exists = exists
        
    def __repr__(self):
        if not self.exists is None:  
            return "DROP {} {} {}".format('TABLE', self.exists, self.table)
        return "DROP {} {}".format('TABLE', self.table)

class Create(Sql):
    
    def __init__(self, table, fields, constraints = [], **kwargs):
        super().__init__('CREATE', table, fields, constraints, **kwargs)
        self.table = table
        self.fields = fields
        self.constraints = constraints
        
    def __repr__(self):
        return 'CREATE TABLE {}({}{})'.format(self.table, 
                                            self.fields,
                                            ', '.join(self.constraints))
    

class Union(Sql):
    
    def __init__(self, *args, **kwargs):
        '''
        :Description:
            Concats 
        '''
        

class Select(Sql):
    
    def __init__(self, table, fields, 
                 condition='', **kwargs):
        '''
        :Description:
        :Parameters:
            - **kwargs: dict;
                - precallback: method; passed to the Transaction object which passes
                  it to the sqlobj (adapter) .select() statement
                - postcallback: method; passed to the Transaction object which passes
                  it to the sqlobj (adapter) .select() statement. Adding a method 
                  here which garners the kwargs.get('result') call will fetch 
                  results of the statement.
        '''
        super().__init__('SELECT', table=table, fields=fields, **kwargs)
        self.table = table
        self.fields = fields
        self.existsflag = False
        self.condition = condition
        if isinstance(self.condition, Exists):
            # FIXME: Bad coding practice, may get rid of
            self.existsflag = True
        self.lastqueryresults = ''
        
    def __repr__(self):
        if self.existsflag:
            return '''SELECT EXISTS({} FROM {} {})'''.format( 
             self.fields, self.table)
            
        return '''SELECT {} FROM {} {}'''.format( 
             self.fields, self.table, self.condition) 
        
class Insert(Sql):
    def __init__(self, table, field, values, *args, **kwargs):
        '''
        :Description:
            Constructor for Insert Sql Objects
        
        :Parent:
            Sql
        
        :Parameters:
            - table; Table(): Sql Object with Table name
            - fields; Fields(): Sql Object with column names
            - values; list: List of Sql Value() Objects
        '''
        super().__init__('INSERT', table, field, values, *args, **kwargs)
        self.table = table
        self.field = field
        self.values = values
        
    def __repr__(self):
        mf = self.field
        if self.field.fields != '':
            mf = '{}{}{}'.format(' (', mf, ')')
        return "INSERT INTO {}{} VALUES ({})".format(self.table, 
                                mf,
                                ', '.join(str(x) for x in self.values).strip())
    
class Delete(Sql):
    def __init__(self, table, *args, **kwargs):
        '''
        :Parameters:
            - **kwargs; dict
                - condition; Where object
        '''
        super().__init__('DELETE', table, *args, condition=kwargs.get('condition', None))
        self.table = table
        self.condition = kwargs.get('condition', '')
        
    def __repr__(self):
        return "DELETE FROM {} {}".format(self.table, self.condition)
        
class Update(Sql):
    def __init__(self, table, fields, values, *args, **kwargs):
        super().__init__('UPDATE', table, fields, values, *args,
                         condition=kwargs.get('condition', None))
        self.table = table
        self.field = fields
        self.values = values
        self.condition = kwargs.get('condition', '')
        
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
                raise InvalidSqlValueException(
                    'Non-Equal fields [1] to values [{}] ratio'.format(
                        len(self.values)))
            return "UPDATE {} SET {} = {}{}".format(self.table, ', '.join(params), cond)
        for i in range(0, len(self.values)):
            params.append(self.__parse_values(self.field.fields[i], self.values[i]).strip())
        
        return "UPDATE {} SET {}{}".format(self.table, ', '.join(params), cond)      
     
class Where(Condition):
    
    def __init__(self, field, operator, value, **kwargs):
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
            - conditions: list; Either: 
                - A list of strings or
                - A list of Condition and/or Select objects, where the representation of each
                  additional object is prepended with an "AND" keyword
                (Where('x', '=', '5'), Where('y', '=', '7')) would equal:
                WHERE x = 5 AND y = 7
                If a substring was placed into the condition, it could look like this:
                (Where('x', '=', '5'), Select('t', 'y', Where('y', '=', '7')))
                >> WHERE x = 5 AND (SELECT y FROM t WHERE y = 7)
            - **kwargs: dict;
                - conditions: list; list of Condition objects to append to Where clause
                - operand: 'AND' or 'OR', when multiple conditions are given, they are
                  separated by this operand string. Applies to additional Where objects only
                
        '''
        super().__init__(field, operator, value)
        self.operand = kwargs.get('operand', 'AND')
        self.conditions = kwargs.get('conditions', [])
        if not isinstance(operator, str):
            raise InvalidSqlConditionException('Operator not valid: {}'.format(operator))

        
        if not isinstance(self.conditions, list) and \
           not isinstance(self.conditions, str) and \
           not isinstance(self.conditions, Condition):
            raise InvalidSqlConditionException('Invalid parameter {}'.format(str(self.conditions)))
        
        if not isinstance(self.conditions, list):
            self.conditions = [self.conditions]
    
    def condition(self):
        '''
        :Description:
            Returns the Where values (except the optional conditional additions) 
            in a Condition() object.
        '''
        return Condition(self.field, self.operator, self.value)
        
    def __repr__(self):
        conditions = '{}'.format(' '.join(
            str(cond) for cond in self.conditions).replace("WHERE", self.operand))
        return "WHERE {} {} {} {}".format(self.field, self.operator,
                                          self.value, conditions).strip()
        
class WhereIn(Where):
    '''
    :Description:
        WhereIn is a child object of Where that configures the Condition
        to specify multiple values in Sql-Acceptable formats.
        It makes specifying a list of values much easier.
    '''
    
    def __init__(self, field, values):
        super().__init__(field, 'IN', self.formatValues(values))
        
    def formatValues(self, values):
        if isinstance(values, Value):
            return "{}".format(tuple(values.value))
        elif isinstance(values, list) or isinstance(values, tuple):
            if len(values) == 1:
                return "({})".format(Value(values[0]))
            return "{}".format(tuple(values))
        elif isinstance(values, Select):
            return "({})".format(str(values)) # Returns sql query
        
class Order(Condition):
    '''
    :Description:
        Add a condition to organize rows. Only applicable on Select objects.
    '''
    
    def __init__(self, *args, **kwargs):
        '''
        :Parameters:
            - **kwargs: dict; 
                - 'fields' : Field object
                - 'collate' : boolean
                - 'sort' : string 'ASC' or 'DESC', case insensitive
                - 'nocase' : boolean
        '''
        super().__init__(kwargs.get('fields', ''),
                         '', '')
        self.fields = kwargs.get('fields', '')
        self.collate = kwargs.get('collate', '')
        self.nocase = kwargs.get('nocase', '')
        self.sort = kwargs.get('sort', '')
        self.args = args

    def __eq__(self, other):
        return str(self) == str(other) 

    def __repr__(self):
        # To prevent massive misunderstandings of the Order object, 
        # allow arguments to contain strings that get ordered for ease of use.
        
        def space(string):
            if string:
                return ' {}'.format(string)
            return ''
        
        if self.args:
            
            self.fields = '{}{}'.format(self.fields, Fields(', '.join(self.args)))
        return "ORDER BY{}{}{}{}".format(space(self.fields), 
                                          space(self.collate), 
                                          space(self.nocase),
                                          space(self.sort))
        
class Exists(Condition):
    '''
    :Description:
        Appends the IF EXISTS or IF NOT EXISTS sql condition when handling 
        tables or columns.
    '''
    
    def __init__(self, exists=True, conditions = []):
        super().__init__('', '', '')
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
    
class Value(Sql):
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
    def __init__(self, *args, **kwargs):
        '''
        :Parameters:
            - **kwargs; dict:
                - sanitize; bool: Convert quotes into unicode glyphs. Works on single quotes
        '''
        if len(args) < 1:
            raise InvalidSqlValueException('Non-existant values in initialization')
        else:
            self.value = args
        wastuple = False
        if isinstance(self.value, tuple):
            self.value = list(self.value)
            wastuple = True
            
        if kwargs.get('sanitize_quotes', False):
            for i in range(len(self.value)):
                if isinstance(self.value[i], str):
                    self.value[i] = self.value[i].replace("'", "U+0027")
        if wastuple:
            self.value = tuple(self.value)
            # We want to keep tuple format, but it cannot be changed unless it's
            # a list. We reconvert it here after any modifications are possible
        
        
    def __repr__(self):
        if isinstance(self.value, tuple) or isinstance(self.value, list):
            if len(self.value) == 1:
                value = self.value[0]
                if isinstance(value, dt.datetime):
                    return "DATE('{}')".format(value.strftime("%Y-%M-%D %H-%m-%S"))
                elif isinstance(value, str):
                    return "'{}'".format(value)
                return "{}".format(value)
        if isinstance(self.value, dt.datetime):
            return "'{}'".format(self.value.strftime("%Y-%M-%D %H-%m-%S"))
        return str(self.value) # FIXME
#         if isinstance(self.value, int):
#             return str(self.value)
#         elif isinstance(self.value, list):
#             return ', '.join(self.value)
#         elif isinstance(self.value, tuple):
#             if len(self.value) == 1:
#                 self.value = "({})".format(self.value[0])
#             else:
#                 return ', '.join(str(self.value))
#         elif isinstance(self.value, str):
#             return "'{}'".format(self.value)
#         return "{}: {}".format(self.value, type(self.value))
    
class Type(Sql):
    '''
    :Description:
        Defines a specific type for the database to recognize
        
        If a Type object is not specified, the python variable instancetype 
        is used instead.
    '''
    def __init__(self, typename, *args, **kwargs):
        self.typename = typename
   
    def __repr__(self):
        return '{}'.format(self.typename)
    
class Key(Sql):
    '''
    :Description:
        Defines an attribute of a field column specified in the
        Table() object.
    '''
    def __init__(self):
        pass
        
class PrimaryKey(Key):
    '''
    :Description:
        Defines a field as the primary key of the table
        
    :Parameters:
        
        FIXME: Not implemented
    '''
    def __init__(self, order='', *args, **kwargs):
        pass
    
    def __repr__(self):
        return 'PRIMARY KEY'
    
class ForeignKey(Key):
    '''
    :Description:
        Defines a table and corresponding field to be a Foreign Key
        
        FIXME: Not implemented
    '''
    
    def __init__(self, table, field, *args, **kwargs):
        self.table = table
        self.field = field
        
    
    
class Table(Sql):
    '''
    :Description:
        A class that represents a table name.
    '''
    def __init__(self, table):
        self.table = table
        
    def __repr__(self):
        return str(self.table)
    
class Field(Sql):
    '''
    :Description:
        Wrapper around column name with data type and Key() data
    '''
    
class Fields(Sql):
    '''
    :Description:
        Class representation of field lists in a class for more
        functionality. Represented by a comma-delimited string
        of values
    '''
    
    def __init__(self, *args, **kwargs):
        '''
        :Parameters:
            - *args: list[string]; name of columns/fields
            - **kwargs: dict;
                - 'distinct': list[string] field names
                  Example: Fields('x', distinct=['x']) == SELECT DISTINCT x FROM tabl
        '''
        # A Wildcard eliminates the need for any additional fields
        self.distinct = kwargs.get('distinct', [])
        self.__reload_distinction__()
        if len(args) == 0:
            args = '' # Empty, so INSERT statements don't fail, need empty string
        elif '*' in args:
            args = ['*']
        else:
            if not type(args) in [list, tuple]:
                raise InvalidSqlValueException(
                        'Field Value is neither a wildcard char nor a list or tuple')
        self.fields = args
        
    def __reload_distinction__(self):
        if self.distinct:
            if isinstance(self.distinct, str):
                self.distinct = [self.distinct]
            elif isinstance(self.distinct, list):
                pass
            else:
                raise InvalidDistinctFieldFormat('Unique or Distinct Field is invalid: {}'.format(
                                                str(self.distinct)))
                
    def __repr__(self):
        self.__reload_distinction__()
        nondistinctfields = []
        if self.distinct:
            for f in self.fields:
                if not f in self.distinct:
                    nondistinctfields.append(f)
        # Separate distinct from non-distinct field parameters
        if len(nondistinctfields) > 0:
            return '{}{}'.format('DISTINCT ({}), '.format(', '.join(self.distinct)),
                                 ', '.join(nondistinctfields))
        else:
            if len(self.distinct) > 0:
                return 'DISTINCT ({})'.format(', '.join(self.fields))
            # Returning fields only
            return '{}'.format(', '.join(self.fields))



class Group(Sql):
    '''
    :Description:
        Organises fields in Select statements based on input parameters
    '''
    def __init__(self, *args, **kwargs):
        '''
        :Parameters:
            - *args: list[string]; field / column names
        '''
        self.fields = args
        self.having = kwargs.get('having')
        
    def __repr__(self):
        return 'GROUP BY {}'.format(', '.join(self.fields))
    

class Having(Where):
    
    def __init__(self, field, operator, value, **kwargs):
        '''
        :Description:
            This is the equivalent of a Where clause applied to 
            a group object.
            
        '''
        super().__init__(field, operator, value, **kwargs)
        
    def __repr__(self):
        conditions = '{}'.format(' '.join(
            str(cond) for cond in self.conditions).replace("WHERE", self.operand))
        return "HAVING {} {} {} {}".format(self.field, self.operator,
                                          self.value, conditions).strip()

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
