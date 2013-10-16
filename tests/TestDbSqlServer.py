#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author: Daniel Kettle
# Date:   July 29 2013
#

### NOTE ###
# This Entire test assumes that an empty database named "test" has been created
# and is accessible to a trusted user.

import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
import squallsql as sql
import squall
import squallserver

class Test(unittest.TestCase):

    sqlobj = None

    @classmethod
    def setUpClass(cls):
        cls.sqlobj = sql.SqlAdapter(driver='squallserver')
        cls.sqlobj.Connect(**{'server':'localhost', 'adapter':'sqlserver', 
                               'trusted':True, 'driver':'SQL Server',
                               'database':'master'})

    def setUp(self):        
        assert not self.sqlobj is None, 'squallserver not imported correctly or invalid' 
        self.createtransaction = self.sqlobj.Transaction()
        assert not self.createtransaction is None, 'Transaction object not instantiated'
        fieldx = self.sqlobj.SQL.get('Field')('x', datatype='INTEGER', 
                                              nullable=False, 
                                              key=self.sqlobj.SQL.get("PrimaryKey")())
        fieldy = self.sqlobj.SQL.get('Field')('y', datatype='INTEGER')
        fieldz = self.sqlobj.SQL.get('Field')('z', datatype='INTEGER')
        create = squall.Create(self.sqlobj.SQL.get('Table')("test"), 
                               self.sqlobj.SQL.get('Fields')(fieldx, fieldy, fieldz), []);
        self.sqlobj.sql(str(create))
        self.sqlobj.Commit()
        
    def testSqlServerValue(self):
        value = self.sqlobj.SQL['Value'](10) # Test value
        self.assertTrue(isinstance(value, squallserver.SqlAdapter.Value), 'Not the desired Value object')
        
        value = self.sqlobj.SQL.get('Value')(10, forcetype='INTEGER', null=True)
        self.assertEqual(str(value), "10 INTEGER NULL", 
                         'SqlServer Value got unexpected value {}'.format(value))
        
    def testCreateDatabase(self):
        create = squall.Create("test", squall.Fields('x', 'y', 'z'), []);
        
    
    def testDropDatabase(self):
        drop = squall.Drop("test")
         
#         self.createtransaction.add(Verbatim("SELECT * FROM sys.tables WHERE name = 't'"))
        #vobj = Verbatim("""IF NOT EXISTS(SELECT * FROM sys.tables WHERE name = 't') CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))""")
#         vobj = Verbatim("CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))")
#         self.createtransaction.add(vobj)
#         assert not self.createtransaction is None, 'Transaction object is None'
#         self.createtransaction.run()
#         
#     def testDropAndCreate(self):
#         self.createtransaction.clear()
#         self.createtransaction.add(Verbatim('DROP TABLE t'),
#                                    Verbatim("""IF NOT EXISTS(SELECT * FROM sys.tables WHERE name = 't') CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))"""))
#         self.createtransaction.run()
         
#     def testInsert(self):
#         print(self.Insert(self.Table('t'), 
#                                                self.Fields(), 
#                                                [self.Value(2), self.Value(2), self.Value(3)]))
#         self.createtransaction.add(self.Insert(self.Table('t'), 
#                                                self.Fields(), 
#                                                [self.Value(2), self.Value(2), self.Value(3)]))
#         self.createtransaction.run()
#         
#     def testSelect(self):
#         print("Test: Select Insert Statement")
#         # , self.sqlselect, self.sqldelete
#         
#         #sqltran = tsql.Transaction(self.sqlobj, self.sqlinsert)
#         print(self.sqlselect)
#  
#     def testInsertAndDelete(self):
#         print("Test: Inserting test data into t table")
#         self.createtransaction.clear()
#         self.assertRaises(EmptyTransactionException, self.createtransaction.run)
#         self.createtransaction.add(self.sqlinsert, self.sqlselect, self.sqldelete)
#         print("Test: Selecting data we inserted")
#         self.createtransaction.run()
#  
#     def testUpdate(self):
#         self.createtransaction.clear()
#         self.createtransaction.add(self.sqlobj, self.sqlinsert, self.sqlupdate)
#         newselect = Select(Table('t'), Fields('*'), Where('z', '=', Value(9)))
#         self.createtransaction.add(newselect, self.sqldelete)
#         self.createtransaction.run()
#         
#         self.sqlobj.insert('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', (5, 4, 3))
#         self.sqlobj.update('UPDATE t SET y = ? WHERE x = ?', (9999, 5))
#         self.sqlobj.select('SELECT x, y, z FROM t WHERE y = 9999', ())
#         self.sqlobj.delete('DELETE FROM t WHERE x = 5', ())
        
#     def testConditionExists(self):
#         notcondition = self.Exists(exists=False)
#         self.assertEqual('IF NOT EXISTS', str(repr(notcondition)), 'Condition does not match up with expected string')
#         condition = self.Exists(exists=True)
#         self.assertEqual('IF EXISTS', str(repr(condition)), 'Condition does not match up with expected string')
        
    def tearDown(self):
        self.createtransaction.clear()
        drop = squall.Drop("test")
        # Drop database
        self.sqlobj.sql(str(drop))
        self.sqlobj.Commit()

    @classmethod
    def tearDownClass(cls):
        cls.sqlobj.Disconnect()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()