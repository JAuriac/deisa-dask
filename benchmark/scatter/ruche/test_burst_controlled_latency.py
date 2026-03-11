# Detects scheduler ability to absorb a burst of simultaneous metadata updates

import argparse
import numpy as np
import os
import time
from mpi4py import MPI
from dask.distributed import Client


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-file")
    parser.add_argument("--data-size", type=int)
    parser.add_argument("--run-id")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    client = Client(scheduler_file=args.scheduler_file)

    data = np.ones(
        (args.data_size, args.data_size, args.data_size),
        dtype=np.float64
    )

    # Ensure all clients connected
    comm.Barrier()

    start = time.perf_counter_ns()

    future = client.scatter(data, direct=True)
    future.release()

    end = time.perf_counter_ns()

    latency_ns = end - start

    os.makedirs("res", exist_ok=True)

    out_path = (
        f"res/scatter_times_"
        f"{size}_{args.data_size}_{args.run_id}_{rank}.csv"
    )

    np.savetxt(out_path, [latency_ns], delimiter=",")

    client.close()


if __name__ == "__main__":
    main()
