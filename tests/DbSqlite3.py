#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
from squall import Table, Fields, Value, Where, Verbatim
import squallsql, sqlite3

class Test(unittest.TestCase):
    
    driver = squallsql.SqlAdapter(driver='squallsqlite3', 
                                  database='rfid.db')

    def setUp(self):
        self.driver.Connect(database='rfid.db')
        assert not self.driver is None, 'Driver is not initialized'
        assert not self.driver.sqladapter is None, 'Sql Adapter not initialized'
        trans = self.driver.Transaction() # Requires the sql() method
        # FIXME: Transaction should call squallsqlite3 driver which fills in adapter
        trans.add(Verbatim('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));'))
        trans.run()

    def tearDown(self):
        trans = self.driver.Transaction()
        trans.add(Verbatim('DROP TABLE IF EXISTS t;'))
        trans.run()
        self.driver.Disconnect()
         
    def testTransaction(self):
        trans = self.driver.Transaction()
        assert not trans is None, 'Transaction object is None'
        
        
    def testSqliteAdapter(self):
        assert not self.driver.sqladapter is None, 'Sql Adapter not initialized'
        # Must have a connected sql adapter before this will pass
        assert not self.driver.sqladapter.conn is None, 'Connection not initialized'
        assert not self.driver.sqladapter.cursor is None, 'Cursor not initialized'
        
    def testInsertDeleteUpdate(self):
        print("Test: Sqlite3 Select, Insert, Delete and Update")
        trans = self.driver.Transaction()
        self.sqlselect = self.driver.Select(Table('t'), Fields('*'), Where('x', '=', Value(1), []))
        assert not self.sqlselect is None, 'Select() object not initialized'
        trans.add(self.sqlselect)
        self.sqlinsert = self.driver.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)])
        assert not self.sqlinsert is None, 'Insert() object not initialized'
        trans.add(self.sqlinsert)
        self.sqldelete = self.driver.Delete(Table('t'), Where('x', '=', Value(1)))
        assert not self.sqldelete is None, 'Delete() object not initialized'
        trans.add(self.sqldelete)
        self.sqlupdate = self.driver.Update(Table('t'), Fields('y', 'z'), 
                                              (Value(5), Value(9)), 
                                               Where('x', '=', Value(1)))
        assert not self.sqlupdate is None, 'Update() object not initialized'
        trans.add(self.sqlupdate)
        trans.run()
         
    def testSqliteInsert(self):
        print("Test: Sqlite3 Insert")
        self.sqlinsert = self.driver.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)])
        self.assertEqual(str(self.sqlinsert), 'INSERT INTO t VALUES (1, 2, 3)', 'Unexpected sql string from insert object')
        # TODO: Add non-committed insert, attempt select on it (expect fail), then insert and select (success)
          
    def testSqliteDelete(self):
        print("Test: Sqlite3 Insert and Delete")
        trans = self.driver.Transaction(self.driver.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)]),
                                        self.driver.Delete(Table('t'), Where('x', '=', Value(1))))
        trans2 = self.driver.Transaction(self.driver.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)]),
                                        self.driver.Delete(Table('t'), Where('x', '=', Value(1))))
        trans.run()
        print("Test: Sqlite3 force rollback on Insert/Delete transaction")
        self.assertRaises(sqlite3.IntegrityError, trans2.run, force='rollback')
          
    def testSqliteUpdate(self):
        print("Test: Sqlite3 Insert and Update")
        self.driver.Transaction(self.driver.Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)])).run()
        self.driver.Transaction(self.driver.Update(Table('t'), Fields('y', 'z'), 
                                              (Value(5), Value(9)), 
                                               Where('x', '=', Value(1)))).run()
         
    def testSelectReturn(self):
        print("Testing return value of Select")
        t = self.driver.Transaction(self.driver.Insert(
                Table('t'), Fields(), [Value(1), Value(2), Value(3)]))
        sqlselect = self.driver.Select(Table('t'), Fields('*'), Where('x', '=', Value(1)))
        t.add(sqlselect)
        print(str(sqlselect))
        output = t.run()
        print(str(output))
        assert isinstance(output[str(sqlselect)], list), 'Expected a list, got {}'.format(str(output))
        assert isinstance(output[str(sqlselect)][0], tuple), 'Expected tuple as a result, got {}'.format(type(output[0]))
        
    def testSqlAdapterDriver(self):
        assert not self.driver is None, 'SqlAdapter driver is None'
        
    def testConditions(self):
        pass
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()