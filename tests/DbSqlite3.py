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
        self.s = squall.Session('sqlite3', 'rfid.db')
        self.module = squall.db('sqlite3')
        self.sqlobj = squall.ADAPTERS['sqlite3']
        self.sqlobj.connect('rfid.db')
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));', ())
        

    def tearDown(self):
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        squall.ADAPTERS['sqlite3'].disconnect()
        self.sqlobj = None
        self.s = None
        del self.sqlobj
        del self.s
        
    def testSqliteInsert(self):
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        print("Test: Select Insert Statement")
        assert squall.ADAPTERS.get('sqlite3').select('SELECT * FROM t WHERE x = ?', (1,)), 'Select Statement Errored'
        
    def testSqliteDelete(self):
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        squall.ADAPTERS.get('sqlite3').commit()
        print("Test: Select Insert Statement")
        assert squall.ADAPTERS.get('sqlite3').select('SELECT * FROM t WHERE x = ?', (1,)), 'Select Statement Errored'
        print("Test: Delete Insert")
        assert squall.ADAPTERS.get('sqlite3').delete('DELETE FROM t WHERE x = ?', (1,)), 'Delete Statement Errored'
        squall.ADAPTERS.get('sqlite3').commit()
        
    def testSqliteUpdate(self):
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        squall.ADAPTERS.get('sqlite3').commit()
        
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').update('UPDATE t SET y = ? WHERE x = ?', (10, 1,)), 'Failed Sqlite3 Insert'
        squall.ADAPTERS.get('sqlite3').commit()
        
        print("Test: Select Insert Statement")
        assert squall.ADAPTERS.get('sqlite3').select('SELECT y FROM t WHERE x = ?', (1,)), 'Select Statement Errored'
        
        print("Test: Delete Insert")
        assert squall.ADAPTERS.get('sqlite3').delete('DELETE FROM t WHERE x = ?', (1,)), 'Delete Statement Errored'
        squall.ADAPTERS.get('sqlite3').commit()
        
    def testRollback(self):
        print("Test: Sqlite3 Insert")
        assert squall.ADAPTERS.get('sqlite3').insert('INSERT INTO t VALUES (?, ?, ?)', (1, 2, 3,)), 'Failed Sqlite3 Insert'
        self.assertRaises(self.module.IntegrityError, squall.ADAPTERS.get('sqlite3').rollback)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()