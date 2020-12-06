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

def foo(A, i, x):
    print("foo:", A, id(A), flush=True)
  
    print("foo begins", flush=True)
    print("foo: A=>", A, flush=True)
    A[i] = A[i] + x
    print("foo2: A=>", A, flush=True)


def bar(A, i, x):
    print("bar:", A, id(A), flush=True)
  
    print("bar begins", flush=True)
    print("bar: A=>", A, flush=True)
    A[i] = A[i] + x
    print("bar2: A=>", A, flush=True)


def main():
    N = 1
    shape = (N,)
    dtype = 'float64'
   
    A = torc.shm_alloc(shape, dtype)
    print(A)

    # primary task initializes array A on rank 0
    for i in range(0, N):
        A[i] = 1

    print('before submitting', A)

    q = 0
    q = (q+1) % torc.num_nodes()
    f1 = torc.submit(foo, A, 0, 10, qid=q)
    torc.waitall()
    q = (q+1) % torc.num_nodes()
    f2 = torc.submit(bar, A, 0, 100, qid=q)
    torc.waitall()
    q = (q+1) % torc.num_nodes()
    f3 = torc.submit(foo, A, 0, 1000, qid=q)
    torc.waitall()
    q = (q+1) % torc.num_nodes()
    f4 = torc.submit(bar, A, 0, 10000, qid=q)
    torc.waitall()

    print("main: A=>", A, flush=True)


if __name__ == '__main__':
    torc.start(main)
