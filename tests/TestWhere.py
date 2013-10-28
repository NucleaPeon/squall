'''
Created on 2013-08-19

@author: radlab
'''
import unittest

from squall import *
class Test(unittest.TestCase):

    where = None
    y = 'hello world'
    
    def setUp(self):
        self.where = Where('x', '=', Value(self.y))


    def tearDown(self):
        self.where = None


    def testSingleCondition(self):
        assert self.where == "WHERE x = {}".format(Value(self.y)), 'Failed to properly parse where single clause'
        
    def testWhereAndOr(self):
        newwhere = Where('x', '=', Value(5), operand='OR', conditions=[Where('y', '>', 'x')])
        assert newwhere == 'WHERE x = 5 OR y > x', 'Where OR failed'
        newwhere.operand = 'AND'
        assert newwhere == 'WHERE x = 5 AND y > x', 'Where AND failed'
        
    def testWhereAndOrder(self):
        self.where = Where('x', '=', Value(self.y), conditions=Order(fields=Fields('x'), sort='ASC'))
        
    def testTwoWhereConditions(self):
        # Where('x', '=', '5'), Where('y', '=', '7')
        w = Where('abc', '=', Value('foo'),
                  conditions=self.where)
        assert w == "WHERE abc = 'foo' AND x = 'hello world'", 'Where Clause with two wheres failed'

    def testOrder(self):
        # Proper test:
        o = Order(fields=Fields('x'))
        print(o)
        assert o == 'ORDER BY x', 'Unexpected Order result'
        o.sort = 'ASC'
        print(str(o))
        assert o == 'ORDER BY x ASC', 'Unexpected Sort result'
        o = Order('x', sort='DESC')
        assert o == 'ORDER BY x DESC', 'Unexpected Sort result using unordered arguments'

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()