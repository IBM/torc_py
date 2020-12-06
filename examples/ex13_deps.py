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

def foo(x):
    print("foo begins", flush=True)
    time.sleep(3)
    print("foo ends", flush=True)
    return x + 1

def bar(x):
    print("bar begins", flush=True)
    print("bar ends", flush=True)
    return x**2

def main():
    # get the first future

    f1 = torc.submit(foo, 2, qid=1)
    f2 = torc.submit(bar, 3, deps=[f1])

    torc.waitall()

    # get the final answer, 
    print(f2.result())
    # 9

if __name__ == '__main__':
    torc.start(main)
