'''
Created on Sep 26, 2013

@author: Thinkpad420
'''
import unittest
import squallsql
from squall import *

class Test(unittest.TestCase):

    driver = squallsql.SqlAdapter(driver='squallsqlite3')

    def setUp(self):
        # Use setup from TestDbSqlite3
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


    def testUpdate(self):
        insert = Insert(Table('t'), 
                        Fields('x', 'y', 'z'),
                        [Value('1'), Value('2'), Value('3')])
        trans = self.driver.Transaction()
        print(str(insert))
        trans.add(insert)
        trans.run()
        
        update = Update(Table('t'),
                        Fields('y', 'z'),
                        [Value('5'), Value('10')],
                        condition=None)
        
        trans.add(update)
        trans.run()
        
        query = Select(Table('t'), Fields('*'),
                       condition=Where('y', '>', Value(2)))
        trans.add(query)
        output = trans.run()
        print(output)
        
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()