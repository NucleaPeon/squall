'''
Created on Jul 29, 2013

@author: dkettle
'''
import unittest
import squall
from squall import AdapterException, ADAPTERS
from adapters.squallsqlite3 import SqliteAdapter as sqlite3

class Test(unittest.TestCase):


    def setUp(self):
        # Sqlite3 database
        
        self.s = squall.Session('sqlite3', 'rfid.db')


    def tearDown(self):
        self.s = None
        del self.s


    def testConnections(self):
        print("Test: Broadcast")
        assert self.s.broadcast == True, 'Broadcast is not set to initial value'
        
        print("Test: localhost by default")
        assert len(self.s.pool['localhost']) == 1, 'Invalid number of localhost entries'
        print("Test: sqlite3 by default")
        assert len(self.s.pool['localhost']['sqlite3']), 'Invalid number of sqlite3 entries'
        print("Test: Pool Initialization")
        assert self.s.pool['localhost']['sqlite3']['rfid.db'] == None, 'Created Connection is not initialized'
        
        
    def testAdapter(self):
        '''
        Throws an Unknown Database Type Exception, sqlite2 is not in the dictionary 
        of acceptable connections
        '''
        print("Test: Unknown Database Type")
        self.assertRaises(AdapterException, self.s.connect, 'sqlite2', 'rfid.db', 'localhost')

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()