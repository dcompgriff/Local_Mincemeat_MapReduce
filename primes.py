# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 18:44:22 2016

@author: DAN
"""


#!/usr/bin/env python
import mincemeat
    

#Generate a list of numbers from 2 to 1 million.
data = range(2, 1000000)
data = map(lambda item: str(item), data)

#The data must be enumerated in a dictionary, with integer keys so that the 
#mincemeat code works. This is bc mincemeat expects inputs with an integer 
#as key.
datasource = dict(enumerate(data))

# Chunk data for faster processing.
currentKey = 0
i = 0
tempDict = {}
for key in datasource.keys():
    if i % 100 == 0:
        currentKey += 1
        tempDict[currentKey] = []
        tempDict[currentKey].append(datasource[key])
        i += 1
    else:
        tempDict[currentKey].append(datasource[key])
        i += 1
datasource = tempDict

'''
Method uses
'''            
def isPrime(numTest):
    pass

def mapfn(k, v):
    '''
    Tail recursive isPalindrome test.
    '''
    def isPalindrome(strTest):
        if len(strTest) == 1 or len(strTest) == 0:
            return True
        else:
            if strTest[0] == strTest[-1]:
                return isPalindrome(strTest[1:-1])
            else:
                return False
    '''
    Determine if a number is prime, and yield it if true.
    '''    
    #Yield an amount for each key.
    for value in v:
        if isPalindrome(value):
            yield value, 1


def reducefn(k, vs):
    '''
    Determine if a number is a palindrome.
    '''
    #result = sum(vs)
    return vs#result
    

# Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

# Run the mapreduce job, and store the result in "results".
results = s.run_server(password="changeme")

# Do more work here to calculate the final outputs. 
