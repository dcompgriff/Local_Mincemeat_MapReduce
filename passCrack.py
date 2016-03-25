# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 22:53:43 2016

@author: DAN
"""

#!/usr/bin/env python
import mincemeat
import time
import sys

'''
This map function uses a dfs scheme to generate all possible passwords and their corresponding 
hashes. By using dfs with a root string that can be set, these map functions for generating 
all possible passwords can be distributed to multiple workers in the map-reduce 
cluster.
'''
def mapfn(k, v):
    import hashlib    
    
    def genComb(passwordHash, rootStr = "", maxSize = 4):   
        stack = expandItem(rootStr)
        hashList = []
        while len(stack) > 0:
            item = stack.pop()
            #If first few entries in the hash equal the password hash, yield it.
            hasher = hashlib.md5()
            hasher.update(item)
            hashStr = hasher.hexdigest()[0: len(passwordHash)]
            if hashStr == passwordHash:
                hashList.append(('found', item))
            
            #Expand the set of items in the combination set.
            if len(item) < maxSize:
                for st in expandItem(item):
                    stack.append(st)
        return hashList

    def expandItem(item):
        charArray = range(97, 123)
        charArray2 = range(48, 58)
        charArray.extend(charArray2)
        charArray = map(lambda item: chr(item), charArray)
        retArray = [] 
        for char in charArray:
            retArray.append(char + item)
        
        return retArray    
    
    '''
    Determine if a number is prime, and yield it if true.
    '''         
    #Generate all combinations/permutations given the starting root, using dfs form.  
    hashList = genComb(v[1], rootStr=v[0])
                    
    for item in hashList:
        yield item[0], item[1]
    
def reducefn(k, vs):
    return str(vs)

'''
This method is the same as the expand method in the map function. It is duplicated so that 
this function can be used to initialize the set of data passed to the map-reduce 
workers.
'''
def initialChars(item):
    charArray = range(97, 123)
    charArray2 = range(48, 58)
    charArray.extend(charArray2)
    charArray = map(lambda item: chr(item), charArray)
    retArray = [] 
    for char in charArray:
        retArray.append(char + item)
    
    return retArray

#Initialize the datasource.
if len(sys.argv) <= 1:
    sys.exit("Not enough arguments!")
#Store password hash and generate initial data set.
passwordHash = sys.argv[1]#'d077f'
singleChars = initialChars("")
data = map(lambda item: (item, passwordHash), singleChars)
datasource = dict(enumerate(data))

#Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

# Run the mapreduce job, and store the result in "results".
print("Attacking " + passwordHash)
startTime = time.time()
#Generate all possible passwords that hash to a given value.
results = s.run_server(password="changeme")
endTime = time.time()
fullTime = (endTime - startTime)
print("Time to run code: " + str(fullTime) + " seconds.")
print("Results: ")
print(str(results))