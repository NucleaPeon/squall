import sys, os
sys.path.append(os.path.join(os.getcwd(), '..'))
sys.path.append(os.path.join(os.getcwd(), '..', 'adapters'))

import unittest
import squall

class Test(unittest.TestCase):
    
    def setUp(self):
        self.sqlobj = squall.Session().connect('rfid.db', adapter='sqlite3')
        self.module = squall.db('sqlite3')
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        self.sqlobj.sql('CREATE TABLE t(x INTEGER, y, z, PRIMARY KEY(x ASC));', ())
        
    def tearDown(self):
        self.sqlobj.sql('DROP TABLE IF EXISTS t;', ())
        self.sqlobj.disconnect()
        del self.sqlobj
    
    def testCreation(self):
        # Invalid Sql() object
        self.assertRaises(squall.InvalidConditionException, 
            squall.Sql, 'select', table='t', fields=['x', 'y', 'z'], conditions=['hello', 2])
        
        self.assertRaises(squall.InvalidSqlCommandException, 
            squall.Sql, 'superselect', table='t')
        
        obj = squall.Sql('insert', table='t', fields=['x', 'y', 'z'], values=[5, 7, 9])
        print(str(obj))
#         self.assertEqual(str(obj), 'INSERT INTO t (x, y, z) VALUES 5, 7, 9)',
#                          'Insert Sql object does not match with expected results')
        
        
    
    def testSqlStatement(self):
        pass
    
    def testSelect(self):
        pass
    
    def testInsert(self):
        pass
    
    def testDelete(self):
        pass
    
    def testUpdate(self):
        pass
    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()