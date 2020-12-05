"""
(C) Copyright IBM Corporation 2020
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Explicit global memory variable using MPI shared memory
"""
import torcpy as torc
import time
from mpi4py import MPI
import numpy as np
from functools import partial
import ctypes

A = None

def alloc_mem(shape, dtype):
    global A

    dt = np.dtype(dtype)
    print(dt)
    itemsize = np.dtype(dtype).itemsize
    size = np.prod(shape)
    if torc.node_id() == 0:
        nbytes = size * itemsize 
    else: 
        nbytes = 0

    win = MPI.Win.Allocate_shared(nbytes, itemsize, comm=MPI.COMM_WORLD) 
    buf, itemsize = win.Shared_query(0) 
    A = np.ndarray(buffer=buf, dtype=dtype, shape=shape) 
    return

def foo(x):
    global A
   
    print("foo begins", flush=True)
    print("foo: A=>", A, flush=True)
    A[x] = A[x] + 99*(x+1)
    print("foo2: A=>", A, flush=True)
    return x + 1


def main():
    global A
    N = 10
    shape=(N,)
    dtype = 'float64' 

    torc.spmd(alloc_mem, shape, dtype) 

    # primary task initializes array A on rank 0
    for i in range(0, N):
        A[i] = 100*i

    f1 = torc.submit(foo, 1)
    f2 = torc.submit(foo, 2)
    f3 = torc.submit(foo, 3)
    f4 = torc.submit(foo, 4)
    torc.waitall()

    print(f1.result())
    print(f2.result())
    print(f3.result())
    print(f4.result())
    print("main: A=>", A, flush=True)


if __name__ == '__main__':
    torc.start(main)
