"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Implicit global memory variable using MPI shared memory
"""
import torcpy as torc
import time
from mpi4py import MPI
import numpy as np
import ctypes

def foo(A, x):
    print(A, flush=True)
  
    print("foo begins", flush=True)
    print("foo: A=>", A, flush=True)
    A[x] = A[x] + 99*(x+1)
    print("foo2: A=>", A, flush=True)
    return x + 1


def main():
    N = 10
    shape = (N,)
    dtype = 'float64'
   
    A = torc.shm_alloc(shape, dtype)
    print(A)

    # primary task initializes array A on rank 0
    for i in range(0, N):
        A[i] = 100*i

    print('before submitting', A)

    f1 = torc.submit(foo, id(A), 1)
    f2 = torc.submit(foo, id(A), 2)
    f3 = torc.submit(foo, id(A), 3)
    f4 = torc.submit(foo, id(A), 4)
    torc.waitall()

    print(f1.result())
    print(f2.result())
    print(f3.result())
    print(f4.result())
    print("main: A=>", A, flush=True)


if __name__ == '__main__':
    torc.start(main)
