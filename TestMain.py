'''
Created on Sep 18, 2013

@author: Daniel Kettle
'''
from subprocess import Popen, PIPE

def testTest(testPath):
    testPath = 'tests/{}'.format(testPath)
    popen = Popen(['python', '-m', 'unittest', testPath], stdout=PIPE,
                                                          stderr=PIPE)
    stdout, stderr = popen.communicate()
    stderr = stderr.decode("utf-8")
    stdout = stdout.decode("utf-8")
    return stdout, stderr

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    import os
    dirs = os.listdir(path='tests')
    tests = {}
    for d in dirs:
        # Fixme: use regular expression Test____.py
        if 'Test' in d:
            tests[d] = testTest(d)[1]
    fails = 0
    for test, val in tests.items():
        #Fixme: use regular expression to get time and print that out
        success = 'OK' in val
        print('{}: {}'.format(test, success))
        if not success:
            fails += 1
            
    if fails > 0:
        print("Number of Failed Tests: {}".format(fails))
        
    
        
        
            
    