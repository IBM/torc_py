"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Master-worker demo, adapted from torc-lite
"""
import time
import torcpy as torc
import threading


def work(x):
    time.sleep(1)
    y = x**2
    print("work inp={:.3f}, out={:.3f} ...on node {:d} worker {} thread {}".format(x, y, torc.node_id(),
                                                                                  torc.worker_id(),
                                                                                  threading.get_ident()), flush=True)
    return y


def main():
    # tr.print_diff()

    ntasks = 4  # 100000
    sequence = range(1, ntasks + 1)

    t0 = torc.gettime()
    tasks = []
    for i in sequence:
        task = torc.submit(work, i)
        tasks.append(task)
    torc.wait()
    t1 = torc.gettime()

    for t in tasks:
        print("Received: {}^2={:.3f}".format(t.input(), t.result()))

    del tasks

    # tr.print_diff()

    print("Elapsed time={:.2f} s".format(t1 - t0))


if __name__ == '__main__':
    torc.start(main)
    # tr.print_diff()
