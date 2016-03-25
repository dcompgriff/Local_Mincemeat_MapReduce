# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 18:44:22 2016

@author: DAN
"""


#!/usr/bin/env python
import mincemeat
import time
    

#Generate a list of numbers from 2 to 1 million.
data = range(2, 10000000)
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
    if i % 1000 == 0:
        currentKey += 1
        tempDict[currentKey] = []
        tempDict[currentKey].append(datasource[key])
        i += 1
    else:
        tempDict[currentKey].append(datasource[key])
        i += 1
datasource = tempDict
tempDict = None
data = None



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
            yield value, value


def reducefn(k, vs):
    '''
    Method uses
    '''            
    def isPrime(numTest):
        import math
        valNum = int(numTest)
        for m in range(2, int(math.ceil(valNum**.5)) + 1):
            if valNum % m == 0:
                return False
            else:
                return True
    '''
    Determine if a number is a palindrome.
    '''
    for value in vs:
        if isPrime(value):
            return True
        else:
            return False

#Set up the "Server", which is really the code to run on the mapreduce workers or "Client".
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

# Run the mapreduce job, and store the result in "results".
startTime = time.time()
#Generate all numbers from 2 to 10,000,000 that are prime palindromes.
results = s.run_server(password="changeme")
#Pull just the numbers from the set of returned numbers.
primes = map(lambda item2: item2[0], filter(lambda item: item[1], results.iteritems()))
endTime = time.time()
fullTime = (endTime - startTime) / 60.0
print("Time to run code: " + str(fullTime))
print("Results: ")
print(str(primes))
print("Number or primes (Not Including 2) : " + str(len(primes)))
