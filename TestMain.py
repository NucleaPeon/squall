'''
Created on Sep 18, 2013

@author: Daniel Kettle
'''
import unittest

import tests.TestConditions as TestConditions
import tests.TestDbSqlite3 as TestDbSqlite3
import tests.TestDbSqlServer as TestDbSqlServer
import tests.TestFields as TestFields
import tests.TestWhere as TestWhere 
class Test(unittest.TestCase):
    
    def testConditions(self):
        suite = TestConditions.unittest.TestLoader().loadTestsFromModule(TestConditions)
        unittest.TextTestRunner(verbosity=2).run(suite)
        
    def testDbSqlite3(self):
        suite = TestConditions.unittest.TestLoader().loadTestsFromModule(TestDbSqlite3)
        unittest.TextTestRunner(verbosity=2).run(suite)
        
    def testDbSqlServer(self):
        suite = TestConditions.unittest.TestLoader().loadTestsFromModule(TestDbSqlServer)
        unittest.TextTestRunner(verbosity=2).run(suite)
        
    def testFields(self):
        suite = TestConditions.unittest.TestLoader().loadTestsFromModule(TestFields)
        unittest.TextTestRunner(verbosity=2).run(suite)
        
    def testWhere(self):
        suite = TestConditions.unittest.TestLoader().loadTestsFromModule(TestWhere)
        unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    unittest.main()
        
    
        
        
            
    