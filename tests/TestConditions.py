'''
Created on 2013-08-20

@author: radlab
'''
import unittest
from squall import Condition, Value, Where, Exists, Order, Fields, Having, WhereIn, InvalidSqlConditionException

class Test(unittest.TestCase):

    cond = Condition('x', '>=', Value(5))

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def formatValues(self, values):
        newlist = []
        if isinstance(values, str):
            values = [values]
        else:
            if  isinstance(values, list) or \
                isinstance(values, tuple):
                if len(values) == 0:
                    raise InvalidSqlConditionException('Cannot create condition without values')
            elif isinstance(values, dict):
                values = values.items()
            else:
                raise InvalidSqlConditionException(
                        'WhereIn only accepts string/list/tuple or dict objects')
        for v in values:
            newlist.append(Value(v))
            
        return "{}".format(tuple(newlist))



    def testCondition(self):
        self.assertEqual(str(self.cond), 'x >= 5', 'Invalid Condition object value')
        
    def testWhereCondition(self):
        self.assertEqual(Where('x', '>=', Value(5)), 'WHERE x >= 5',
                         'Invalid Where object value')
        self.assertEqual(Where('x', '>=', Value(5)).condition(), self.cond,
                         'Invalid Where -> Condition object value')
        
    def testExistsCondition(self):
        exists = Exists(True)
        assert isinstance(exists, Condition), 'Exists is not a condition'
    
    def testOrderCondition(self):
        order = Order(fields=Fields('x'), sort='ASC')
        assert isinstance(order, Condition), 'Exists is not a condition'
        
    
    def testHavingCondition(self):
        having = Having('x', ">=", Value(5))
        assert isinstance(having, Condition), 'Having is not a condition'
        self.assertEqual(str(having), 'HAVING x >= 5', 'Having does not match expected value')

    def testWhereIn(self, ids = ['1d619a81-8b86-401f-9c39-46c2942a939d',
                                 '3edbe095-d6b1-41b5-93b0-4d4f1d4ed4e0',
                                 'b8d1de9d-1473-4ebf-a032-5399f0763cd7',
                                 '5d86e2f8-2f35-457c-a027-76a9b101928f',
                                 '7fe6a51c-7133-4b84-b9da-978c864601f8']):
        condition = WhereIn(Fields('DocumentDefinitionId'), ids)
        assert condition, 'Condition is False or None'        
        ids = ('1d619a81-8b86-401f-9c39-46c2942a939d',
               '3edbe095-d6b1-41b5-93b0-4d4f1d4ed4e0',)
        
        v = Value(ids) # FIXME: Error will occur if a Value() object is used
        condition = WhereIn(Fields('DocumentDefinitionId'), v)
        assert condition, 'Condition is False or None'

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()