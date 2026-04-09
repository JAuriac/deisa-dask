# Detects scheduler ability to absorb a burst of simultaneous metadata updates

import argparse
import os
import time
from mpi4py import MPI
from dask.distributed import Client


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-file")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    client = Client(scheduler_file=args.scheduler_file)

    data = 1.0

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
        f"{size}_{args.run_id}_{rank}.csv"
    )

    # Save a single value
    with open(out_path, "w") as f:
        f.write(f"{latency_ns}\n")

    client.close()


if __name__ == "__main__":
    main()
