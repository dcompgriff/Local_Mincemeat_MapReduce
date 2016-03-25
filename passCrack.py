# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 22:53:43 2016

@author: DAN
"""

#!/usr/bin/env python
import mincemeat
import time
import hashlib
    

#Generate a list of numbers from 2 to 1 million.
hasher = hashlib.md5()
#hasher.update('string')
#hasher.hexdigest()[0:5]
#97 - 122, 48 - 57
passwordHash = 'd077f'

data = {num: password for num in range(0, 4)}


#The data must be enumerated in a dictionary, with integer keys so that the 
#mincemeat code works. This is bc mincemeat expects inputs with an integer 
#as key.
datasource = dict(enumerate(data))

def mapfn(k, v):
    '''
    Determine if a number is prime, and yield it if true.
    '''    
    charArray = range(97, 123)
    charArray2 = range(48, 58)
    charArray.extend(charArray2)    
    
    hasher = hashlib.md5()  
    
    
    #For each character position, generate a new password hash.
    for i0 in range(0, 1):
        pass
        
        
    #After all permutations have been generated, add the hash of the password and the password to the list.
    #If an array has an item, the reduce it.
    yield v, 1
    
        
    
    
def reducefn(k, vs):
        
    
    return vs
    

def genComb(rootStrList = "", maxSize = 4):
    #TODO: Add code to initialize the stack and run dfs for each root str.    
    stack = expandItem(rootStrList)
    while len(stack) > 0:
        item = stack.pop()
        print(item)
        if len(item) < maxSize:
            for st in expandItem(item):
                stack.append(st)


def expandItem(item):
    charArray = range(97, 123)
    charArray2 = range(48, 58)
    charArray.extend(charArray2)
    charArray = map(lambda item: chr(item), charArray)
    retArray = [] 
    for char in charArray:
        retArray.append(char + item)
    
    return retArray

    


#Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
#s = mincemeat.Server()
#s.datasource = datasource
#s.mapfn = mapfn
#s.reducefn = reducefn
#
## Run the mapreduce job, and store the result in "results".
#startTime = time.time()
##Generate all numbers from 2 to 10,000,000 that are prime palindromes.
#results = s.run_server(password="changeme")
#endTime = time.time()
#fullTime = (endTime - startTime) / 60.0
#print("Time to run code: " + str(fullTime))
#print("Results: ")
#print(str(results))