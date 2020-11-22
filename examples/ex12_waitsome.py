"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Explicit task management with submit and wait
"""
import torcpy as torc
import time

def work1(x):
    print("work1 begins", flush=True)
    time.sleep(2)
    print("work1 finishes", flush=True)
    return x * x


def work2(x):
    print("work2 begins", flush=True)
    time.sleep(4)
    print("work2 finishes", flush=True)
    return x * x


def main():
    t1 = torc.submit(work1, 10)
    t2 = torc.submit(work2, 20)

    # torc.wait()
    print("waiting for tasks...", flush=True)
    torc.wait(tasks=[t1])
    print("after wait(t1)", flush=True)
    torc.wait(tasks=[t2])
    print("after wait(t2)", flush=True)
    print(t1.result())
    print(t2.result())


if __name__ == '__main__':
    torc.start(main)
