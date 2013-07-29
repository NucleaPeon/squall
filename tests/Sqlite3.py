'''
Created on Jul 29, 2013

@author: dkettle
'''
import unittest, os
import sqlite3 as sqlite3

class Test(unittest.TestCase):


    def setUp(self):
        print("Initializing Database")
        self.conn = sqlite3.connect('rfid.db')
        self.cursor = self.conn.cursor()
        self.conn.row_factory = sqlite3.Row


    def tearDown(self):
        print("Closing Database")
        self.conn.close()

    def testIfDatabaseExists(self):
        assert os.path.exists(os.path.join(os.getcwd(), 'rfid.db')) == True, 'Database does not exist'
        
    def testInsertAndDeleteStatement(self):
        print("Inserting Temporary Data into DB")
        sql = '''INSERT INTO Document (Id, DocumentDefinitionId) VALUES (?, ?)'''
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe235', 'ee7c11d6-1de3-4cc1-8792-0958a49fe236',)
        self.cursor.execute(sql, param)
        self.conn.commit()
        print("Selecting from DB")
        sql = 'SELECT Id, DocumentDefinitionId FROM Document WHERE Id=?;'
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe235', )
#         print(str(self.cursor.execute(sql, param).fetchall()))
        print("Fetching results of Select statement")
        print(str(self.cursor.execute(sql, param).fetchall()[0]))
        assert self.cursor.execute(sql, param).fetchall()[0] == ('ee7c11d6-1de3-4cc1-8792-0958a49fe235', 'ee7c11d6-1de3-4cc1-8792-0958a49fe236'), 'Could not select Document Id'
        print("Deleting Temporary Data")
        assert self.cursor.execute('DELETE FROM Document WHERE Id = ?;', ('ee7c11d6-1de3-4cc1-8792-0958a49fe235',)), 'Could not delete Document by Id'
        self.conn.commit()

    def testSelectStatement(self):
        sql = '''SELECT DocumentDefinitionId FROM Document WHERE Id = ?;'''
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe234',)
        result = 'cbfbcaaa-5d9d-4ad6-b591-a563b97c8948'
        assert self.cursor.execute(sql, param).fetchall()[0][0] == result, 'DocumentDefinitionId not found'
    
    def testUpdateStatement(self):
        print("Inserting Temporary Data into DB")
        sql = '''INSERT INTO Document (Id, DocumentDefinitionId) VALUES (?, ?)'''
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe235', 'ee7c11d6-1de3-4cc1-8792-0958a49fe236',)
        self.cursor.execute(sql, param)
        self.conn.commit()
        print("Updating Values")
        sql = '''UPDATE Document SET DocumentDefinitionId=? WHERE Id=?'''
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe239', 'ee7c11d6-1de3-4cc1-8792-0958a49fe235',)
        self.cursor.execute(sql, param)
        self.conn.commit()
        
        print ("Checking Updated Values")
        sql = '''SELECT DocumentDefinitionId FROM Document WHERE Id=?'''
        param = ('ee7c11d6-1de3-4cc1-8792-0958a49fe235',)
        results = self.cursor.execute(sql, param).fetchall()[0][0]
        assert results == 'ee7c11d6-1de3-4cc1-8792-0958a49fe239', 'Updated failed to change value' 

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()