"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Simple example of callbacks
"""
import time
import torcpy as torc
import threading


def cb(task):
    tid = threading.get_ident()
    inp = task.input()
    out = task.result()
    print("thread {}: callback with arg={}".format(tid, inp), flush=True)


def work(x):
    tid = threading.get_ident()
    time.sleep(1)
    y = x*x
    print("thread {}: work inp={}, out={} ... on node {}".format(tid, x, y, torc.node_id()), flush=True)
    return y


def main():
    ntasks = 4
    sequence = range(1, ntasks+1)

    t0 = torc.gettime()
    t_all = []
    for i in sequence:
        task = torc.submit(work, i, callback=cb)
        t_all.append(task)
    torc.wait()
    t1 = torc.gettime()

    for task in t_all:
        print("Received: {}^2={}".format(task.input(), task.result()))

    print("Elapsed time={:.2f} s".format(t1-t0))


if __name__ == '__main__':
    torc.start(main)
