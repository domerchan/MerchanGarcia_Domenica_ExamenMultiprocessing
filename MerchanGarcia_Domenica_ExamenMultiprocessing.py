#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 09:37:09 2020

@author: Do
"""

import time
import numpy as np
import functools
from multiprocessing import Process, Manager
import copy


def how_many_within_range_sequential(row, minimum, maximum):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count += 1
    return count

def how_many_within_range_parallel(arr, minimum, maximum, res):
    for row in arr:
        count = 0
        for n in row:
            if minimum <= n <= maximum:
                count += 1
        res.append(count)

if __name__ == '__main__':
    np.random.RandomState(100)
    arr = np.random.randint(0, 10, size=[4000000, 10])
    ar = arr.tolist()
    #print(arr)
    
    
    inicioSec = time.time()
    resultsSec = []
    for row in arr:
        resultsSec.append(how_many_within_range_sequential(row, minimum=4, maximum=8))
    finSec = time.time()
    resultsSec.sort()
    #print(resultsSec)
    
    
    
    
    numProcesos = 8
    filas = np.size(arr,0) // numProcesos
    residuo = np.size(arr,0) % numProcesos
    inicio = 0
    
    inicioPar = time.time()
    with Manager() as manager:
        res = manager.list()
        procesos = [] 
        for p in range(numProcesos):
            numFilas = filas + 1 if p < residuo else filas
            #print()
            #print(arr[inicio:inicio+numFilas])
            proceso = Process(target=how_many_within_range_parallel, \
                              args=(arr[inicio:inicio+numFilas], 4, 8, res))
            proceso.start()
            procesos.append(proceso)
            inicio += numFilas
        for p in procesos:
            p.join()
            p.terminate
        finPar = time.time()
        resultsPar = copy.deepcopy(res)
    resultsPar.sort()
    #print(resultsPar)
    
    
    print('Results are correct!\n' if functools.reduce(lambda x, y : x and y, map(lambda p, q: p == q,resultsSec,resultsPar), True) else 'Results are incorrect!\n')
    print('Sequential Process took %.3f ms \n' % ((finSec - inicioSec)*1000))
    print('Parallel Process took %.3f ms \n' % ((finPar - inicioPar)*1000))
    
    
    
    
    
    