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
    parser.add_argument("--duration", type=int)
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    stop_time = time.time() + args.duration

    latencies = []

    data = np.ones(
        (args.data_size, args.data_size, args.data_size),
        dtype=np.float64
    )

    while time.time() < stop_time:
        try:
            client = Client(scheduler_file=args.scheduler_file, timeout="2s")

            start = time.perf_counter_ns()
            future = client.scatter(data, broadcast=True)
            client.gather(future)
            end = time.perf_counter_ns()

            latencies.append(end - start)

            client.close()

        except Exception:
            break

    if latencies:
        mean_latency = float(np.mean(latencies))
        success_count = len(latencies)
    else:
        mean_latency = np.nan
        success_count = 0

    os.makedirs("res", exist_ok=True)

    out_path = (
        f"res/scatter_times_"
        f"{size}_{args.data_size}_None_{rank}.csv"
    )

    np.savetxt(out_path,
               [[mean_latency, success_count]],
               delimiter=",")


if __name__ == "__main__":
    main()
