'''
Created on Jul 29, 2013

@author: dkettle
'''
import unittest
import squall

class Test(unittest.TestCase):


    def setUp(self):
        # Sqlite3 database
        
        self.s = squall.Session('sqlite3', 'rfid.db')


    def tearDown(self):
        del self.s


    def testName(self):
        assert self.s.broadcast == True, 'Broadcast is not set to initial value'


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()