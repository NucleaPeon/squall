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
        # Sqlite3 database
        self.sqlobj = squall.Session().connect('rfid.db', adapter='sqlite3')
        self.module = squall.db('sqlite3')
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));', ())
        

    def tearDown(self):
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        self.sqlobj.disconnect()
        del self.sqlobj
        
    def testSqliteInsert(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        print("Test: Select Insert Statement")
        assert self.sqlobj.select('SELECT * FROM t WHERE x = ?', (1,)), 'Select Statement Errored'
        
    def testSqliteDelete(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
        print("Test: Select Insert Statement")
        rows = self.sqlobj.select('SELECT * FROM t WHERE x = ?', (1,))
        assert len(rows) > 0, 'Select Statement Errored'
        print("Test: Delete Insert")
        assert self.sqlobj.delete('DELETE FROM t WHERE x = ?', (1,)), 'Delete Statement Errored'
        self.sqlobj.commit()
        
    def testSqliteUpdate(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
        
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.update('UPDATE t SET y = ? WHERE x = ?', (10, 1,)), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
        
        print("Test: Select Insert Statement")
        assert self.sqlobj.select('SELECT y FROM t WHERE x = ?', (1,)), 'Select Statement Errored'
        
        print("Test: Delete Insert")
        assert self.sqlobj.delete('DELETE FROM t WHERE x = ?', (1,)), 'Delete Statement Errored'
        self.sqlobj.commit()
        
    def testRollback(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        self.assertRaises(self.module.IntegrityError, squall.ADAPTERS.get('sqlite3').rollback)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()