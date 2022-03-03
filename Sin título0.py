#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:07:40 2022
@author: alumno
"""


#Implementar un merge concurrente:
#- Tenemos NPROD procesos que producen números no negativos de forma
#creciente. Cuando un proceso acaba de producir, produce un -1
#
#- Hay un proceso merge que debe tomar los números y almacenarlos de
#forma creciente en una única lista (o array). El proceso debe esperar a que
#los productores tengan listo un elemento e introducir el menor de
#ellos.
#
#- Se debe crear listas de semáforos. Cada productor solo maneja los
#sus semáforos para sus datos. El proceso merge debe manejar todos los
#semáforos.
#
#- OPCIONAL: mente se puede hacer un búffer de tamaño fijo de forma que
#los productores ponen valores en el búffer.


from multiprocessing import Process, Manager
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Array#, Value
from time import sleep
from random import random, randint

NCONS = 1 # la cantidad de consumidores es 1
NPROD = 3 # la cantidad de productores es P
N = 10 # la cantidad de productos que va a producir cada productor
K = 1 # el k del bounded semaphore (la capacidad de almacenamiento)


def delay(factor = 3):
    sleep(random()/factor)


def add_data(storage, pid, mutex):
    mutex.acquire()
    try:
        storage[pid] = storage[pid] + randint(0,5)
        delay(6)
        '''storage[index.value] = pid*1000 + data
        delay(6)
        index.value = index.value + 1'''
    finally:
        mutex.release()


def producer(storage, empty, non_empty, mutex):
    pid = int(current_process().name.split('_')[1])
    for _ in range(N): # produces N veces en total
        print (f"producer {current_process().name} produciendo")
        delay(6)
        empty.acquire()
        add_data(storage, pid, mutex)
        non_empty.release()
        print (f"producer {current_process().name} almacenado")
    mutex.acquire()
    storage[pid] = -1 # ya ha producido las N veces

'''
def producer(storage, index, empty, non_empty, mutex):
    for v in range(N):
        print (f"producer {current_process().name} produciendo")
        delay(6)
        empty.acquire()
        add_data(storage, index, int(current_process().name.split('_')[1]),
                 v, mutex)
        non_empty.release()
        print (f"producer {current_process().name} almacenado {v}")
'''
def min_raro(n,idn,m,idm): # queremos que devuelva el minimo y el indice del minimo
    if n == -1 and m == -1:
        result, idr = -1, -1
    elif n == -1: # n == -1 != m
        result, idr = m, idm
    elif m == -1:
        result, idr = n, idn # m == -1 != n
    else: # m != -1 != n
        if n > m:
            result, idr = m, idm
        else:
            result, idr = n, idn
    return result, idr
        

def get_min(storage, mutex,running):
    data, idd = -1, -1
    index_running = []
    for i in [i for i in range(NPROD) if running[i]]:
        if storage[i] == -1:
            running[i] = False
        else:
            index_running.append(i)
    
    amount_index = len(index_running)
    
    mutex.acquire()
    
    try:
        if amount_index == 1:
            idd = index_running[0]
            data = storage[idd]
        elif amount_index > 1:
            for i in range(amount_index-1): # tomamos los indices validos
                i1 = index_running[i]
                i2 = index_running[i+1]
                data, idd = min_raro(storage[i1],i1,storage[i2],i2)
                delay()
        '''data = storage[0]
        index.value = index.value - 1
        delay()
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1'''
    finally:
        mutex.release()
    return data, idd
'''
def get_data(storage, index, mutex):
    mutex.acquire()
    try:
        data = storage[0]
        index.value = index.value - 1
        delay()
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1
    finally:
        mutex.release()
    return data
'''

def true_in_lista(l):
    i = 0
    long = len(l)
    hay_true = False
    while (not hay_true) and i < long:
        hay_true = l[i]
        i += 1
    return hay_true


def consumer(storage, empties, non_empties, mutex, merge):
    for ne in non_empties:
        ne.acquire()
        
    running = [True for _ in range(NPROD)]
    
    while true_in_lista(running):
#        for ne in non_empties:
#            ne.acquire()
        print (f"consumer desalmacenando")
        dato, idd = get_min(storage, mutex,running)
        merge.append(dato)
        print (f"consumer consumiendo {dato}")
        empties[idd].release() # hacemos signal al productor del que hemos tomado
                               # para que pueda producir
        delay()
'''
def consumer(storage, index, empty, non_empty, mutex):
    for v in range(N):
        non_empty.acquire()
        print (f"consumer {current_process().name} desalmacenando")
        dato =get_data(storage, index, mutex)
        empty.release()
        print (f"consumer {current_process().name} consumiendo {dato}")
        delay()
'''

def main():
    storage = Array('i', K*NPROD)
    #index = Value('i', 0)           # no nos hace falta porque K = 1
    for i in range(K*NPROD):
        storage[i] = 0              # elegimos que el primer valor sea 0
    #print ("almacen inicial", storage[:], "indice", index.value)
    
    manager = Manager()
    merge = manager.list()
    #index = Value('i', 0)   # indice del merge
    
    '''
    manager1 = Manager()
    manager2 = Manager()
    non_empties = manager1.list() # lista de semaforos non_empties de tamaño <- deberia ser array
    empties = manager2.list()
    '''
    non_empties = []
    empties = []
    for i in range(NPROD):
        non_empties.append(Semaphore(0))
        empties.append(BoundedSemaphore(K))
    mutex = Lock()

    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        #args=(storage, index, empties[i], non_empties[i], mutex))
                        args=(storage, empties[i], non_empties[i], mutex))
                for i in range(NPROD) ]

    conslst = [ Process(target=consumer,
                      name="cons",
                      #args=(storage, index, empties, non_empties, mutex, merge)) ]
                      args=(storage, empties, non_empties, mutex, merge)) ]

    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()


if __name__ == '__main__':
    main()