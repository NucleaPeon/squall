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
from squallerrors import *

class Test(unittest.TestCase):

    sqlobj = None

    @classmethod
    def setUpClass(cls):
        cls.sqlobj = sql.SqlAdapter(driver='squallserver')
        cls.sqlobj.Connect(**{'server':'localhost', 'adapter':'sqlserver', 
                               'trusted':True, 'driver':'SQL Server',
                               'database':'master'})
        cls.fieldx = cls.sqlobj.SQL.get('Field')('x', datatype='INTEGER', 
                                              nullable=False, 
                                              key=cls.sqlobj.SQL.get("PrimaryKey")())
        cls.fieldy = cls.sqlobj.SQL.get('Field')('y', datatype='INTEGER')
        cls.fieldz = cls.sqlobj.SQL.get('Field')('z', datatype='INTEGER')
        cls.fields = cls.sqlobj.SQL.get('Fields')(cls.fieldx, cls.fieldy, cls.fieldz)
        cls.table = cls.sqlobj.SQL.get('Table')("test")
        cls.create = cls.sqlobj.SQL.get('Create')(cls.table, 
                                                  cls.fields);
                               
        cls.drop = squall.Drop("test")

    def setUp(self):        
        assert not self.sqlobj is None, 'squallserver not imported correctly or invalid' 
        self.createtransaction = self.sqlobj.Transaction()
        assert not self.createtransaction is None, 'Transaction object not instantiated'
        
        self.sqlobj.sql(str(self.create))
        self.sqlobj.Commit()
        
    def testSqlServerValue(self):
        value = self.sqlobj.SQL.get('Value')(10, forcetype='INTEGER', null=True)
        self.assertEqual(str(value), "10 INTEGER NULL", 
                         'SqlServer Value got unexpected value {}'.format(value))
        
    def testTransaction(self):
        self.createtransaction.add(self.drop)
        self.createtransaction.add(self.create)
        self.createtransaction.run()
        
    def testExistsCondition(self):
        exists = self.sqlobj.SQL.get('Exists')(False, """SELECT * FROM sys.tables WHERE name = 't'""", 
                                               self.create)
        self.createtransaction.add(exists)
        self.createtransaction.run()
         
        #self.createtransaction.add(Verbatim("SELECT * FROM sys.tables WHERE name = 't'"))
        #vobj = Verbatim("""IF NOT EXISTS(SELECT * FROM sys.tables WHERE name = 't') CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))""")
#         vobj = Verbatim("CREATE TABLE t(x INTEGER, y INTEGER, z INTEGER, CONSTRAINT x_pk PRIMARY KEY(x))")
#         self.createtransaction.add(vobj)
#         assert not self.createtransaction is None, 'Transaction object is None'
#         self.createtransaction.run()
#         
    def testDropAndCreate(self):
        self.createtransaction.clear()
        self.createtransaction.add(self.drop, self.create)
        self.createtransaction.run()
        
         
    def testInsertAndSelect(self):
        self.createtransaction.clear()
        # --> Insert code
        Value = self.sqlobj.SQL.get("Value")
        fields = self.sqlobj.SQL.get("Fields")("x", "y", "z")
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, fields, [Value(4),
                                                                        Value(3),
                                                                        Value(2)])
        self.createtransaction.add(testinsert)
        # <-- End Insert code
        self.createtransaction.run()
        # Test Insert #
        where = self.sqlobj.SQL.get("Where")('x', '=', Value(4))
        select = self.sqlobj.SQL.get("Select")(self.table, fields, condition=where)
        self.createtransaction.add(select)
        retdata = self.createtransaction.run()[str(select)]
        self.assertGreater(len(retdata), 0, 'Failed to retrieve inserted data')
        
  
    def testInsertAndDelete(self):
        self.createtransaction.clear()
        self.assertRaises(EmptyTransactionException, self.createtransaction.run)
        Value = self.sqlobj.SQL.get("Value")
        fields = self.sqlobj.SQL.get("Fields")("x", "y", "z")
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, fields, [Value(4),
                                                                        Value(3),
                                                                        Value(2)])
        self.createtransaction.add(testinsert)
        self.createtransaction.run()
        where = self.sqlobj.SQL.get("Where")('x', '=', Value(4))
        testdelete = self.sqlobj.SQL.get("Delete")(self.table, 
                                                   condition=where)
        self.createtransaction.add(testdelete)
        self.createtransaction.run()
  
    def testUpdate(self):
        testupdate = self.sqlobj.SQL.get('Update')()
        self.createtransaction.add(testupdate)
         
#         self.sqlobj.insert('INSERT INTO t (x, y, z) VALUES (?, ?, ?)', (5, 4, 3))
#         self.sqlobj.update('UPDATE t SET y = ? WHERE x = ?', (9999, 5))
#         self.sqlobj.select('SELECT x, y, z FROM t WHERE y = 9999', ())
        
    def testExistsRow(self):
        pass
#         self.sqlobj.delete('DELETE FROM t WHERE x = 5', ())
        
#     def testConditionExists(self):
#         notcondition = self.Exists(exists=False)
#         self.assertEqual('IF NOT EXISTS', str(repr(notcondition)), 'Condition does not match up with expected string')
#         condition = self.Exists(exists=True)
#         self.assertEqual('IF EXISTS', str(repr(condition)), 'Condition does not match up with expected string')
        
    def tearDown(self):
        self.createtransaction.clear()
        # Drop database
        self.sqlobj.sql(str(self.drop))
        self.sqlobj.Commit()

    @classmethod
    def tearDownClass(cls):
        cls.sqlobj.Disconnect()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()