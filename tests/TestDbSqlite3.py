#!/usr/bin/env python

#
# Author: Daniel Kettle
# Date:   July 29 2013
#

import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
from squallerrors import *
import squallsql

class Test(unittest.TestCase):
    
    sqlobj = None

    @classmethod
    def setUpClass(cls):
        cls.sqlobj = squallsql.SqlAdapter(driver='squallsqlite3', 
                                          database='rfid.db')
        cls.sqlobj.Connect(database='rfid.db')
        cls.Verbatim = cls.sqlobj.SQL.get('Verbatim')
 
    def setUp(self):
        assert not self.sqlobj is None, 'Driver is not initialized'
        assert not self.sqlobj.sqladapter is None, 'Sql Adapter not initialized'
        trans = self.sqlobj.Transaction() # Requires the sql() method
        # FIXME: Transaction should call squallsqlite3 driver which fills in adapter
        v = self.Verbatim('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));')
        trans.add(v)
        trans.run()
 
    def tearDown(self):
        trans = self.sqlobj.Transaction()
        trans.add(self.Verbatim('DROP TABLE IF EXISTS t;'))
        trans.run()
           
    def testTransaction(self):
        trans = self.sqlobj.Transaction()
        assert not trans is None, 'Transaction object is None'
         
         
    def testSqliteAdapter(self):
        assert not self.sqlobj.sqladapter is None, 'Sql Adapter not initialized'
        # Must have a connected sql adapter before this will pass
        assert not self.sqlobj.sqladapter.conn is None, 'Connection not initialized'
        assert not self.sqlobj.sqladapter.cursor is None, 'Cursor not initialized'
         
    def testSelectInsertDeleteUpdate(self):
        trans = self.sqlobj.Transaction()
        self.sqlselect = self.sqlobj.Select(self.sqlobj.Table('t'), self.sqlobj.Fields('*'), 
                                condition=self.sqlobj.Where('x', '=', self.sqlobj.Value(1)))
        assert not self.sqlselect is None, 'Select() object not initialized'
        trans.add(self.sqlselect)
        self.sqlinsert = self.sqlobj.Insert(self.sqlobj.Table('t'), self.sqlobj.Fields(), 
                                            [self.sqlobj.Value(1), 
                                             self.sqlobj.Value(2), 
                                             self.sqlobj.Value(3)])
        assert not self.sqlinsert is None, 'Insert() object not initialized'
        trans.add(self.sqlinsert)
        self.sqldelete = self.sqlobj.Delete(self.sqlobj.Table('t'), 
                                            condition=self.sqlobj.Where('x', '=', self.sqlobj.Value(1)))
        assert not self.sqldelete is None, 'Delete() object not initialized'
        trans.add(self.sqldelete)
        self.sqlupdate = self.sqlobj.Update(self.sqlobj.Table('t'), self.sqlobj.Fields('y', 'z'), 
                                              (self.sqlobj.Value(5), 
                                               self.sqlobj.Value(9)), 
                                               condition=self.sqlobj.Where('x', '=', self.sqlobj.Value(1)))
        assert not self.sqlupdate is None, 'Update() object not initialized'
        trans.add(self.sqlupdate)
        trans.run()
          
    def testSqliteInsert(self):
        self.sqlinsert = self.sqlobj.Insert(self.sqlobj.Table('t'), 
                                            self.sqlobj.Fields(), 
                                            [self.sqlobj.Value(1), 
                                             self.sqlobj.Value(2), 
                                             self.sqlobj.Value(3)])
        self.assertEqual(str(self.sqlinsert), 'INSERT INTO t VALUES (1, 2, 3)', 'Unexpected sql string from insert object')
        # TODO: Add non-committed insert, attempt select on it (expect fail), then insert and select (success)
           
    def testSqliteDelete(self):
        trans = self.sqlobj.Transaction(self.sqlobj.Insert(self.sqlobj.Table('t'), 
                                                           self.sqlobj.Fields(), 
                                                           [self.sqlobj.Value(1), 
                                                            self.sqlobj.Value(2), 
                                                            self.sqlobj.Value(3)]),
                                        self.sqlobj.Delete(self.sqlobj.Table('t'), 
                                                           self.sqlobj.Where('x', '=', self.sqlobj.Value(1))))
        trans2 = self.sqlobj.Transaction(self.sqlobj.Insert(self.sqlobj.Table('t'), 
                                                            self.sqlobj.Fields(), 
                                                            [self.sqlobj.Value(1), 
                                                             self.sqlobj.Value(2), 
                                                             self.sqlobj.Value(3)]),
                                        self.sqlobj.Delete(self.sqlobj.Table('t'), 
                                                           self.sqlobj.Where('x', '=', self.sqlobj.Value(1))))
        trans.run()
        self.assertRaises(RollbackException, trans2.run, force='rollback')
           
    def testSqliteUpdate(self):
        self.sqlobj.Transaction(self.sqlobj.Insert(self.sqlobj.Table('t'), 
                                                   self.sqlobj.Fields(), 
                                                   [self.sqlobj.Value(1), 
                                                    self.sqlobj.Value(2), 
                                                    self.sqlobj.Value(3)])).run()
        self.sqlobj.Transaction(self.sqlobj.Update(self.sqlobj.Table('t'), 
                                                   self.sqlobj.Fields('y', 'z'), 
                                              (self.sqlobj.Value(5), 
                                               self.sqlobj.Value(9)), 
                                               self.sqlobj.Where('x', '=', self.sqlobj.Value(1)))).run()
          
    def testSelectReturn(self):
        t = self.sqlobj.Transaction(self.sqlobj.Insert(
                self.sqlobj.Table('t'), self.sqlobj.Fields(), 
                [self.sqlobj.Value(1), 
                 self.sqlobj.Value(2), 
                 self.sqlobj.Value(3)]))
        sqlselect = self.sqlobj.Select(self.sqlobj.Table('t'), 
                                       self.sqlobj.Fields('*'), 
                                       self.sqlobj.Where('x', '=', self.sqlobj.Value(1)))
        t.add(sqlselect)
        output = t.run()
        assert isinstance(output[str(sqlselect)], list), 'Expected a list, got {}'.format(str(output))
        assert isinstance(output[str(sqlselect)][0], tuple), 'Expected tuple as a result, got {}'.format(type(output[0]))
         

    @classmethod
    def tearDownClass(cls):
        cls.sqlobj.Disconnect()
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
