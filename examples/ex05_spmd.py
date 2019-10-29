"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Switch from master-worker to SPMD and call MPI bcast
"""
import numpy
import torcpy as torc
from mpi4py import MPI

N = 3
A = numpy.zeros(N, dtype=numpy.float64)


def bcast_task(root):
    global A
    comm = MPI.COMM_WORLD
    # Broadcast A from rank 0
    comm.Bcast([A, MPI.DOUBLE], root=root)


def work():
    global A
    print("node:{} -> A={}".format(torc.node_id(), A))


def main():
    global A

    # primary task initializes array A on rank 0
    for i in range(0, N):
        A[i] = 100*i

    torc.spmd(bcast_task, torc.node_id())  # 2nd arg (root) is 0 and can be omitted

    torc.spmd(work)


if __name__ == '__main__':
    torc.start(main)
