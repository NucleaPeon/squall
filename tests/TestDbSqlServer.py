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
from squall import *
import squallsql as sql

class Test(unittest.TestCase):

    createtransaction = None

    def setUp(self):
        self.sqlobj = sql.SqlAdapter(driver='squallserver')
        self.sqlobj.Connect(**{'server':'localhost', 'adapter':'sqlserver', 
                               'trusted':True, 'driver':'SQL Server'})
        
        self.sqlselect = Select(Table('t'), Fields('*'), condition=Where('x', '=', Value(1)))
        self.sqlinsert = Insert(Table('t'), Fields(), [Value(1), Value(2), Value(3)])
        self.sqldelete = Delete(Table('t'), condition=Where('x', '=', Value(1)))
        self.sqlupdate = Update(Table('t'), Fields('y', 'z'), 
                                              (Value(5), Value(9)),
                                              condition=Where('x', '=', Value(1)))
        assert not self.sqlobj is None, 'Squallserver not imported correctly or invalid'
        vbmsql = Verbatim('CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x));')
        self.createtransaction = self.sqlobj.Transaction(vbmsql)
        self.createtransaction.run()
        assert not self.createtransaction is None, 'Transaction object is None'
    
    def testSelect(self):
        print("Test: Select Insert Statement")
        # , self.sqlselect, self.sqldelete
        
        #sqltran = tsql.Transaction(self.sqlobj, self.sqlinsert)
        print(self.sqlselect)
 
    def testInsertAndDelete(self):
        print("Test: Inserting test data into t table")
        self.createtransaction.clear()
        self.assertRaises(EmptyTransactionException, self.createtransaction.run)
        self.createtransaction.add(self.sqlinsert, self.sqlselect, self.sqldelete)
        print("Test: Selecting data we inserted")
        self.createtransaction.run()
 
    def testUpdate(self):
        self.createtransaction.clear()
        self.createtransaction.add(self.sqlobj, self.sqlinsert, self.sqlupdate)
        newselect = Select(Table('t'), Fields('*'), Where('z', '=', Value(9)))
        self.createtransaction.add(newselect, self.sqldelete)
        self.createtransaction.run()
        
#         self.sqlobj.insert('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', (5, 4, 3))
#         self.sqlobj.update('UPDATE t SET y = ? WHERE x = ?', (9999, 5))
#         self.sqlobj.select('SELECT x, y, z FROM t WHERE y = 9999', ())
#         self.sqlobj.delete('DELETE FROM t WHERE x = 5', ())
        
        
    def tearDown(self):
        self.createtransaction.clear()
        vbmsql = Verbatim('DROP TABLE t;')
        self.createtransaction.add(vbmsql).run()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()