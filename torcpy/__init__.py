from .runtime import node_id, num_nodes, worker_local_id, num_local_workers, worker_id, num_workers
from .runtime import submit, wait, map, init, shutdown, launch, start
from .runtime import gettime, spmd, enable_stealing, disable_stealing
from .runtime import finalize, waitall
from .runtime import as_completed
from .runtime import TorcPoolExecutor

name = "torcpy"
