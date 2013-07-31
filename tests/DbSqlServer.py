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
        print("Connecting to SQL Server on local machine")
        self.s = squall.Session('sqlserver', 'rfid')
        self.module = squall.db('sqlserver')
        self.sqlobj = squall.ADAPTERS['sqlserver']
        self.sqlobj.connect('rfid', trusted=True, driver='SQL Server')
        print("Dropping table if it exists...")
        print("Creating Table t")
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))', ())
        assert not self.module is None, 'Python Driver not imported successfully'
        assert not self.sqlobj is None, 'Squallserver not imported correctly or invalid'
        self.sqlobj.commit()
    
    def testSelect(self):
        print("Test: Select Insert Statement")
#         assert squall.ADAPTERS.get('sqlite3').select('SELECT * FROM t WHERE x = ?', (1,)), 'Select Statement Errored'    

    def tearDown(self):
        print("Closing connection to SQL Server on local machine")
        self.sqlobj.sql('DROP TABLE t;', ())

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()