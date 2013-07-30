'''
Created on Jul 30, 2013

@author: dkettle
'''
import unittest, inspect
import squall

import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

class Test(unittest.TestCase):


    def setUp(self):
        # Sqlite3 database
        self.s = squall.Session('sqlite3', 'rfid.db')

    def tearDown(self):
        self.s = None
        del self.s


    def testSqlite3Adapter(self):
        # Before assigned
        print("Test: Connector Initialization")
        assert squall.ADAPTERS.get('sqlite3') is None, 'sqlite3 adapter has been initialized'
        squall.db('sqlite3')
        assert not squall.ADAPTERS['sqlite3'] is None, 'sqlite3 adapter not initialized correctly'
        assert inspect.ismethod(squall.ADAPTERS['sqlite3'].connect), 'API Object has no connector method'
        
    def testSqliteInsert(self):
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').insert('test', ('me',)), 'Failed Sqlite3 Insert'
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()