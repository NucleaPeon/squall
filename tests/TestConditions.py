'''
Created on 2013-08-20

@author: radlab
'''
import unittest
from squall import Condition, Value, Where, Exists, Order, Fields, Having, WhereIn

class Test(unittest.TestCase):

    cond = Condition('x', '>=', Value(5))

    def setUp(self):
        pass


    def tearDown(self):
        pass


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
        print(str(order))
    
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
        print(str(condition))

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()