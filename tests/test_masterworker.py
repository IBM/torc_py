"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""


import torcpy
import time
import os
import sys


def work(x):
    time.sleep(1.0)
    y = x**2
    return y


def main():
    ntasks = 4
    sequence = range(1, ntasks + 1)

    t0 = torcpy.gettime()
    tasks = []
    for i in sequence:
        task = torcpy.submit(work, i)
        tasks.append(task)
    torcpy.wait()
    t1 = torcpy.gettime()
    print("elapsed time={:.3f} s".format(t1-t0))

    for t in tasks:
        inp = t.input()
        res = t.result()
        ref = work(inp)
        assert(res == ref)


def test_masterworker(nworkers):
    os.environ["TORCPY_WORKERS"] = str(nworkers)
    torcpy.start(main)


if __name__ == "__main__":
    test_masterworker(int(sys.argv[1]))
