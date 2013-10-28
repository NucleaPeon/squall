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
        cls.columns = cls.sqlobj.SQL.get("Fields")("x", "y", "z") # Used in Sql objects
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
        exists = self.sqlobj.SQL.get('Exists')(True, """SELECT * FROM sys.tables WHERE name = 'test'""", 
                                               self.drop)
        self.createtransaction.add(exists)
        self.createtransaction.run()
        exists = self.sqlobj.SQL.get('Exists')(False, """SELECT * FROM sys.tables WHERE name = 'test'""", 
                                               self.create)
        self.createtransaction.add(exists)
        self.createtransaction.run()
        
    def testDropAndCreate(self):
        self.createtransaction.clear()
        self.createtransaction.add(self.drop, self.create)
        self.createtransaction.run()
        
         
    def testInsertAndSelect(self):
        self.createtransaction.clear()
        # --> Insert code
        Value = self.sqlobj.SQL.get("Value")
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, 
                                                   self.columns, 
                                                   [Value(4),
                                                    Value(3),
                                                    Value(2)])
        self.createtransaction.add(testinsert)
        # <-- End Insert code
        self.createtransaction.run()
        # Test Insert #
        where = self.sqlobj.SQL.get("Where")('x', '=', Value(4))
        select = self.sqlobj.SQL.get("Select")(self.table, self.columns, condition=where)
        self.createtransaction.add(select)
        retdata = self.createtransaction.run()[str(select)]
        self.assertGreater(len(retdata), 0, 'Failed to retrieve inserted data')
        
  
    def testInsertAndDelete(self):
        self.createtransaction.clear()
        self.assertRaises(EmptyTransactionException, self.createtransaction.run)
        Value = self.sqlobj.SQL.get("Value")
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, 
                                                   self.columns, 
                                                   [Value(4),
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
        Value = self.sqlobj.SQL.get("Value")
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, 
                                                   self.columns, 
                                                   [Value(4),
                                                    Value(3),
                                                    Value(2)])
        self.createtransaction.add(testinsert)
        self.createtransaction.run()
        where = self.sqlobj.SQL.get("Where")('x', '=', Value(4))
        testupdate = self.sqlobj.SQL.get('Update')(self.table, 
                                                   self.sqlobj.SQL.get("Fields")("y", "z"),
                                                   (Value(900), Value(1000),),
                                                   condition=where)
        self.createtransaction.add(testupdate)
        self.createtransaction.run()
        select = self.sqlobj.SQL.get("Select")(self.table, self.columns, condition=where)
        self.createtransaction.add(select)
        retdata = self.createtransaction.run()[str(select)]
        self.assertEqual(len(retdata), 1, 'Did not find updated results')
        row = retdata[0] # get the data
        # Ensure results are indeed updated
        self.assertEqual(row[1], 900, 'y column was not updated to 900')
        self.assertEqual(row[2], 1000, 'z column was not updated to 1000')
        
        
    def testExistsRow(self):
        Value = self.sqlobj.SQL.get("Value")
        where = self.sqlobj.SQL.get("Where")('x', '=', Value(4))
        testinsert = self.sqlobj.SQL.get("Insert")(self.table, 
                                                   self.columns, 
                                                   [Value(4),
                                                    Value(3),
                                                    Value(2)])
        existsinsert = self.sqlobj.SQL.get("Insert")(self.table, 
                                                   self.columns, 
                                                   [Value(5),
                                                    Value(3),
                                                    Value(2)])
        self.createtransaction.add(testinsert)
        self.createtransaction.run()
        
        # IF a row where x = 4 exists, insert row where x = 5, otherwise insert original row
        exists = self.sqlobj.SQL.get('Exists')(True, """SELECT x, y, z FROM test WHERE x = 4""", 
                                               existsinsert)
        self.createtransaction.add(exists)
        self.createtransaction.run()
        exists = self.sqlobj.SQL.get('Exists')(False, """SELECT x, y, z FROM test WHERE x = 4""", 
                                               testinsert)
        self.createtransaction.add(exists) # This does not get called because row where x = 4 exists
        self.createtransaction.run()
        select = self.sqlobj.SQL.get("Select")(self.table, self.columns, 
                                               condition=self.sqlobj.SQL.get("Where")('x', '=', Value(5)))
        self.createtransaction.add(select)
        query = self.createtransaction.run()[str(select)]
        self.assertGreater(len(query), 0, 
                           'Query failed to return inserted rows that get inserted when value exists')
        
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