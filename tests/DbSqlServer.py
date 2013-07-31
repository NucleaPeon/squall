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
        print("Checking driver and connection")
        assert not self.module is None, 'Python Driver not imported successfully'
        assert not self.sqlobj is None, 'Squallserver not imported correctly or invalid'
        print("Creating Table t")
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))', ())
        self.sqlobj.commit()
    
    def testSelect(self):
        print("Test: Select Insert Statement")
        assert self.sqlobj.select('SELECT * FROM t WHERE x = ?', (1,)), 'Select Statement Errored'

    def testInsertAndDelete(self):
        print("Test: Inserting test data into t table")
        self.sqlobj.insert('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', (1, 2, 3))
        self.sqlobj.commit()
        print("Test: Selecting data we inserted")
        self.sqlobj.select('SELECT x, y, z FROM t WHERE x = 1', ())
        # Should error if fails
        self.sqlobj.delete('DELETE FROM t WHERE x = 1', ())
        self.sqlobj.commit()

    def testUpdate(self):
        print("Test: Inserting a record")
        self.sqlobj.insert('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', (5, 4, 3))
        print("Test: Updateding a record")
        self.sqlobj.update('UPDATE t SET y = ? WHERE x = ?', (9999, 5))
        print("Test: Selecting Record")
        self.sqlobj.commit()
        print("Test: Select Statement")
        self.sqlobj.select('SELECT x, y, z FROM t WHERE y = 9999', ())
        self.sqlobj.delete('DELETE FROM t WHERE x = 5', ())
        self.sqlobj.commit()
        
        
    def tearDown(self):
        print("Closing connection to SQL Server on local machine")
        self.sqlobj.sql('DROP TABLE t;', ())
        self.sqlobj.commit()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()