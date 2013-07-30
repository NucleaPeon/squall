'''
Created on Jul 30, 2013

@author: dkettle
'''
import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
import squall

class Test(unittest.TestCase):


    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def testTest(self):
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()