'''
Created on 2013-08-19

@author: radlab
'''
import unittest

from squall import Fields
class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testField(self):
        f = Fields('a', 'b', 'c')
        assert not f is None, 'Field object is none on basic initialization!'
        assert str(f) == 'a, b, c', 'Fields do not expand as expected'
        dist = Fields('a', 'b', 'c', distinct='a')
        assert str(dist) == 'DISTINCT (a), b, c', 'Distinction on one field not valid'
        dist.distinct = ['a', 'b', 'c']
        print(str(dist))
        assert str(dist) == 'DISTINCT (a, b, c)', 'Distinction on all fields not valid'


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testField']
    unittest.main()