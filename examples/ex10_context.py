"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Master-worker demo using context manager and as_completed
"""
import time
import torcpy as torc
import threading


def work(x):
    time.sleep(1)
    if x <= 2:
        time.sleep(2)
    y = x**2
    print("work inp={:.3f}, out={:.3f} ...on node {:d} worker {} thread {}".format(x, y, torc.node_id(),
                                                                                   torc.worker_id(),
                                                                                   threading.get_ident()), flush=True)
    return y


def main():

    with torc.TorcPoolExecutor():
        print('with statement block') 

        ntasks = 4
        sequence = range(1, ntasks + 1)

        t0 = torc.gettime()
        tasks = []
        for i in sequence:
            task = torc.submit(work, i)
            tasks.append(task)

        for t in torc.as_completed(tasks):
            print("1-Received: {}^2={:.3f}".format(t.input(), t.result()))

        t1 = torc.gettime()
        print("1-Elapsed time={:.2f} s".format(t1 - t0))

    print("After first session")

    with torc.TorcPoolExecutor() as executor:
        ntasks = 4
        sequence = range(1, ntasks + 1)

        t0 = torc.gettime()
        tasks = []
        for i in sequence:
            task = executor.submit(work, i)
            tasks.append(task)
        executor.wait()
        t1 = torc.gettime()

        for t in tasks:
            print("2-Received: {}^2={:.3f}".format(t.input(), t.result()))

        print("2-Elapsed time={:.2f} s".format(t1 - t0))

    print("After second session")


if __name__ == '__main__':
    torc.start(main)
