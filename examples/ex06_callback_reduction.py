"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Reduction on master node using callbacks
"""
import torcpy as torc

# import threading

sum_v = 0


def cb(task):
    global sum_v
    arg = task.result()
    sum_v = sum_v + arg


def work(x):
    y = x ** 2
    return y


def main():
    data = range(10)

    tasks = []
    for d in data:
        t = torc.submit(work, d, callback=cb)
        tasks.append(t)

    torc.wait()
    print("Sum=", sum_v)  # should be 285 = 0^2 + 1^2 + ... + 9^2


if __name__ == '__main__':
    torc.start(main)
