'''
Created on 2013-08-20

@author: radlab
'''
import unittest
from squall import Condition, Value, Where, Exists, Order, Fields, Having

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


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()