"""
(C) Copyright IBM Corporation 2019
All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
which accompanies this distribution, and is available at
http://www.eclipse.org/legal/epl-v10.html
"""

"""
Late switch from SPMD to master-worker and parallel image processing
"""

import os
import sys
import time
from PIL import Image
import torcpy as torc

files = []


def get_files(path):
    all_files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            if f.endswith('.ppm') | f.endswith('.jpg') | f.endswith('.jpeg') | f.endswith('.JPEG') | f.endswith('.png'):
                all_files.append(os.path.join(dirpath, f))

    return sorted(all_files)


def work(i):
    global files
    f = files[i]
    with Image.open(f) as im:
        im = im.resize((32,32))
        # do something more

    return None


def main():
    global files

    # SPMD execution: torc_py and MPI initialization
    torc.init()

    # SPMD execution: common global initialization takes place here
    if len(sys.argv) == 1:
        if torc.node_id() == 0:
            print("usage: python3 {} <path>".format(os.path.basename(__file__)))
        torc.shutdown()
        return

    files = get_files(sys.argv[1])

    # Switching to master-worker
    torc.launch(None)

    t0 = time.time()
    _ = torc.map(work, range(len(files)))
    t1 = time.time()
    print("t1-t0=", t1-t0)

    torc.shutdown()


if __name__ == "__main__":
    main()
