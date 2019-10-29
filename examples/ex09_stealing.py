"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Demonstration of work stealing
"""
import time
import torcpy as torc


def work(x):
    time.sleep(1)
    y = x*x
    print("taskfun inp={}, out={} ...on node {:d}".format(x, y, torc.node_id()), flush=True)
    return y


"""
Main test case: All tasks are submitted to rank 1 and the number of ranks ranges from 2 to 17
"""


def main():
    nodes = torc.num_nodes()
    if nodes < 2:
        print("This examples needs at least two MPI processes. Exiting...")
        return
    local_workers = torc.num_local_workers()
    if local_workers > 1:
        print("This examples should use one worker thread per MPI process. Exiting...")
        return

    ntasks = 16
    sequence = range(1, ntasks + 1)

    t0 = torc.gettime()
    t_all = []
    for i in sequence:
        try:
            task = torc.submit(work, i, qid=1)
            t_all.append(task)
        except ValueError:
            print("torc.submit: invalid argument")

    torc.enable_stealing()
    torc.wait()
    t1 = torc.gettime()
    torc.disable_stealing()

    for task in t_all:
        print("Received: {}^2={}".format(task.input(), task.result()))

    print("Elapsed time={:.2f} s".format(t1 - t0))


if __name__ == '__main__':
    torc.start(main)
