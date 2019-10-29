"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Straightforward integration of torc_py with cmaes
"""
# pip install cma
import cma
import time
import torcpy as torc


def rosenbrock(x):
    """Rosenbrock test objective function"""
    n = len(x)
    if n < 2:
        raise ValueError('dimension must be greater one')
    return sum(100 * (x[i] ** 2 - x[i + 1]) ** 2 + (x[i] - 1) ** 2 for i in range(n - 1))


def main():
    es = cma.CMAEvolutionStrategy(2 * [0], 0.5, {'popsize': 8, 'maxfevals': 320, 'verb_disp': 1, 'seed': 3})
    # es = cma.purecma.CMAES(2 * [0], 0.5, popsize=8, maxfevals=16)
    t0 = time.time()
    while not es.stop():
        solutions = es.ask()
        es.tell(solutions, torc.map(rosenbrock, solutions))
        es.logger.add(es)  # write data to disc to be plotted
        es.disp()

    t1 = time.time()

    print(es.result[0])
    print(es.result[1])
    print(t1 - t0)
    cma.plot()


if __name__ == '__main__':
    torc.start(main)
