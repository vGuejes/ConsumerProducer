from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Array
from time import sleep
from random import random, randint

NPROD = 3 # la cantidad de productores es P
N = 10 # la cantidad de productos que va a producir cada productor
K = 1 # el k del bounded semaphore (la capacidad de almacenamiento)
# Esta practica solo funciona para k=1 (no la parte opcional)

def delay(factor = 3):
    sleep(random()/factor)


def add_data(storage, pid,mutex):
    '''
    Esta funcion anade al almacen del productor pid el sigiente numero generado
    por el productor de id pid. Para ello, necesita que el almacen tenga espacio
    (el semaforo empties[pid] debe estar abierto)

    ENTRADA:
    storage: Almacen compartido
    pid: ID del productor, entero entre 0 y NPROD-1
    mutex: Semaforo binario para la exclusion mutua de las diferentes secciones criticas

    FUNCIONAMIENTO:
    Como es la seccion critica del productor, usamos mutex para no entrar en otras
    secciones criticas
    Su funcionamiento es tomar el anterior valor generado (storage[pid]), sumarle
    un entero aleatorio entre 0 y 5 y almacenarlo de nuevo en storage[pid]
    '''
    mutex.acquire()
    try:
        storage[pid] = storage[pid] + randint(0,5)
        delay(6)
    finally:
        mutex.release()


def get_min(storage,mutex,running):
    '''
    Esta funcion toma el menor valor del almacen. Para ello, necesita que
    todos los productores hayan almacenado algun valor (todos los semaforos
    non_empties deben estar abiertos)

    Ademas, esta funcion actualiza el vector running si es necesario

    ENTRADA:
    storage: Almacen compartido
    mutex: Semaforo binario para la exclusion mutua de las diferentes secciones criticas
    running: vector de booleanos en el que running[i] indica si el productor de
        pid = i ha terminado de producir
    
    SALIDA:
    minimo: el valor minimo de storage
    idmin: el indice del productor al que pertenece minimo
        relacion: minimo = storage[idmin]
    
    FUNCIONAMIENTO:
    como es la seccion critica del consumidor, usamos mutex para no entrar en otras
        secciones criticas
    inicializamos idmin y minimo a -1 por si falla algo en esta funcion,
        lo sepamos desde fuera
    inicializamos la lista de los indices validos (los que storage[i]=!-1)
    para cada indice i que verifique running[i]=True, comprobamos storage[i]
        y lo cambiamos si procede. Si no procede, lo añadimos a la lista de 
        indices validos
    por ultimo, tomamos el menos valor de storage[i] para los indices validos
        y devolvemos tanto el indice como el propio valor
    
    ademas, en el ultimo momento cuando storage=[-1,...,-1], esta funcion es
    la que convierte running a [False] para que termine el consumidor
    '''
    mutex.acquire()
    try:
        idmin, minimo = -1, -1
        id_val = [] # indices validos
        for i in range(NPROD):
            if running[i]:
                if storage[i] == -1:
                    running[i] = False
                else:
                    id_val.append(i)
        if len(id_val) == 1:
            idmin = id_val[0]
            minimo = storage[idmin]
        elif len(id_val) > 1:
            idmin = id_val[0]
            minimo = storage[idmin]
            for i in range(1,len(id_val)):
                j = id_val[i]
                if minimo > storage[j]:
                    idmin = j
                    minimo = storage[j]
        else:
            running = [False]
        delay()
    finally:
        mutex.release()
    return minimo, idmin


def producer(storage,empty,non_empty,mutex):
    '''
    Proceso que simula a cada productor. El productor produce N veces
    un entero no negativo y por ultimo produce un -1.
    Cada elemento que produce lo guarda en storage[pid]

    ENTRADA:
    storage: Almacen compartido
    empty: Decide si el productor puede producir: si el almacen esta lleno, el
        productor no puede producir
    non_empty: Avisa al consumidor de que puede consumir: Si el almacen se queda
        vacio, entonces el consumidor no puede comparar entre todos los almacenes
        para consumir
    mutex: Semaforo binario para la exclusion mutua de las diferentes secciones criticas

    VARIABLES:
    pid: ID del productor, entero entre 0 y NPROD-1

    FUNCIONAMIENTO:
    El proceso crea y guarda en el almacen storage[pid] N enteros no negativos
    que crecen. Para poder crear, necesita que haya espacio en el almacen (que
    empty este abierto), y como el espacio del almacen es K=1, siempre que produce
    tiene que avisar de que el almacen esta lleno (cierra non_empty)

    Al final, como ultima seccion critica del productor, deja -1 en el almacen
    para marcar que ha terminado
    '''
    pid = int(current_process().name.split('_')[1])
    for _ in range(N):
        print(f'productor {pid} produciendo')
        delay(6)
        empty.acquire()
        add_data(storage,pid,mutex)
        non_empty.release()
        print(f'productor {pid} almacenando')
    print(f"productor {pid} termino de producir")
    empty.acquire()
    mutex.acquire()
    try:
        storage[pid] = -1 # cuando termina de producir
    finally:
        mutex.release()
    non_empty.release()


def ordenado(l):
    # devuelve si una lista esta ordenada de menor a mayor
    esta_ordenada = True
    i=0
    long = len(l)
    while esta_ordenada and i<long-1:
        esta_ordenada = l[i]<=l[i+1]
        i+=1
    return esta_ordenada


def consumer(storage,empties,non_empties,mutex,merge):
    '''
    Proceso que simula al consumidor.
    En esta practica, el consumidor consume del almacen cuando todos los
    productores han consumido (todos los semaforos non_empty estan abiertos)
    y consume el menor valor no negativo del almacen
    El proceso termina cuando todos los productores han producido N elementos
    y el almacen es de la forma [-1,...,-1]

    ENTRADA:
    storage: Almacen compartido
    empties: Lista de los semaforos empty de los productores
    non_empties: Lista de los semaforos non_ empty de los productores
    mutex: Semaforo binario para la exclusion mutua de las diferentes secciones criticas
    merge: Lista donde dejaremos los valores consumidos. La practica sera correcta
        si merge contiene todos los valores producidos por los productores ordenada
        de menor a mayor
    
    VARIABLES:
    it: Cantidad de iteraciones que hace el bucle. Al final de la practica, por como
        esta programada, debe terminar con it = N*NPROD
    running: vector de booleanos en el que running[i] indica si el productor de
        pid = i ha terminado de producir
    
    FUNCIONAMIENTO:
    mientras algun productor este produciendo, el consumidor esperara a que todos los
        que aun estan produciendo produzcan. Como consumimos de uno en uno y los almacenes
        tienen tamaño 1
        (esto se traduce en que cada vez que consumimos tenemos que avisar al productor
        del que hemos consumido que tiene que producir y una vez produce, podemos volver
        a consumir)
    tomamos el menor dato y el pid del productor que lo produjo
    anadimos el dato a merge
    avisamos al productor pid que tiene que producir
    esperamos a que el productor pid produzca para poder comparar

    una vez termina el bucle, enseñamos en pantalla la lista final merge con todos los
    elementos producidos ordenados y una confirmacion de que esta ordenada
    '''
    it = 0 # iteraciones del bucle
    for ne in non_empties:
        ne.acquire()
    
    running = [True] * NPROD

    while True in running:
        it += 1
        print("consumer desalmacenando")
        dato, idd = get_min(storage,mutex,running)
        if idd != -1:
            if dato != -1:
                merge.append(dato)
                print(f"consumer consumiendo {dato} de prod_{idd} ({it})")
                empties[idd].release()
                non_empties[idd].acquire()
        else:
            running = [False]
    print(merge, len(merge))
    orden = ordenado(merge)
    print(f"La lista merge esta ordenada: {orden}")


def main():
    '''
    VARIABLES:
    storage: Almacen compartido, tiene tamaño K*NPROD = NPROD
    merge: Lista donde dejaremos los valores consumidos. La practica sera correcta
        si merge contiene todos los valores producidos por los productores ordenada
        de menor a mayor
    empties: Lista de los semaforos empty de los productores
    non_empties: Lista de los semaforos non_ empty de los productores
    mutex: Semaforo binario para la exclusion mutua de las diferentes secciones criticas
    prodlist: lista de productores, tiene tamaño NPROD
    conslist: lista de consumidores, tiene tamaño 1
    '''
    storage = Array('i',K*NPROD)
    for i in range(K*NPROD):
        storage[i] = 0 # el primer valor de storage sera 0 + randint(0,5)
    print("almacen inicial", storage[:])

    merge=[]

    non_empties = []
    empties = []

    for i in range(NPROD):
        non_empties.append(Semaphore(0))
        empties.append(BoundedSemaphore(K))

    mutex = Lock()

    prodlist = [ Process(target=producer,
                         name=f'prod_{i}',
                         args=(storage, empties[i], non_empties[i], mutex))
                 for i in range(NPROD) ]
    
    conslist = [ Process(target=consumer,
                         name='cons',
                         args=(storage, empties, non_empties, mutex, merge))]
                # igual se puede quitar el manager de merge
    
    for p in prodlist + conslist:
        p.start()
    
    for p in prodlist + conslist:
        p.join()



if __name__ == '__main__':
    main()
