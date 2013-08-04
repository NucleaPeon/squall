'''
Created on Jul 30, 2013

@author: dkettle
'''
import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
import squall
from squallsql import Table, Fields, Value, Transaction, Where
import squallsqlite3 as squallsqlite3

class Test(unittest.TestCase):


    def setUp(self):
        # Sqlite3 database
        self.sqlobj = squall.Session().connect('rfid.db', adapter='sqlite3')
        self.module = squall.db('sqlite3')
        self.sqlobj.sql('DROP TABLE IF EXISTS t;')
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));')
        
        self.sqlselect = squallsqlite3.Select(Table('t'), Fields('*'), Where('x', '=', Value(1), []))
        self.sqlinsert = squallsqlite3.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)])
        self.sqldelete = squallsqlite3.Delete(Table('t'), Where('x', '=', Value(1)))
        self.sqlupdate = squallsqlite3.Update(Table('t'), Fields('y', 'z'), 
                                              (Value(5), Value(9)), Where('x', '=', Value(1)))

    def tearDown(self):
        self.sqlobj.sql('DROP TABLE IF EXISTS t;')
        self.sqlobj.disconnect()
        del self.sqlobj
        
    def testSqliteInsert(self):
        print("Test: Sqlite3 Insert")
        self.assertEqual(str(self.sqlinsert), 'INSERT INTO t VALUES (1, 2, 3)', 'Unexpected sql string from insert object')
        assert self.sqlobj.insert(str(self.sqlinsert)), 'Failed Sqlite3 Insert'
        print("Test: Select Insert Statement")
        assert self.sqlobj.select(self.sqlselect), 'Select Statement Errored'
         
    def testSqliteDelete(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert(self.sqlinsert), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
        print("Test: Select Insert Statement")
        rows = self.sqlobj.select(self.sqlselect)
        assert len(rows) > 0, 'Select Statement Errored'
        print(str(rows))
        print("Test: Delete Insert")
        
        assert self.sqlobj.delete(self.sqldelete), 'Delete Statement Errored'
        self.sqlobj.commit()
         
    def testSqliteUpdate(self):
        print("Test: Sqlite3 Insert")
         
        assert self.sqlobj.insert(self.sqlinsert), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
         
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.update(self.sqlupdate), 'Failed Sqlite3 Insert'
        self.sqlobj.commit()
         
        print("Test: Select Insert Statement")
        assert self.sqlobj.select(self.sqlselect), 'Select Statement Errored'
         
        print("Test: Delete Insert")
        assert self.sqlobj.delete(self.sqldelete), 'Delete Statement Errored'
        self.sqlobj.commit()
         
    def testTransaction(self):
        print("Test: Transaction Init (no errors)")
        sqltran = Transaction(self.sqlobj, self.sqlinsert, self.sqlselect, self.sqlupdate, self.sqldelete)
        assert str(self.sqlinsert) in str(sqltran), 'Could not find sql  insert object in transaction' 
        print("Test: Empty Transactions")
        sqltran = Transaction(self.sqlobj)
        self.assertRaises(squall.EmptyTransactionException, sqltran.run)
        print("Test: Adding Transactions to empty Transaction")
        sqltran.add(self.sqlinsert)
        sqltran.add(self.sqlselect)
        self.assertRaises(squall.CommitException, sqltran.run, raise_exception=True)
        print("Test: Transaction Init (with errors)")
        self.assertRaises(squall.InvalidSquallObjectException, Transaction, self.sqlobj, self.sqlinsert, Value(3))
                
    def testRollback(self):
        print("Test: Sqlite3 Insert")
        assert self.sqlobj.insert(self.sqlinsert), 'Failed Sqlite3 Insert'
        self.assertRaises(self.module.IntegrityError, squall.ADAPTERS.get('sqlite3').rollback)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()