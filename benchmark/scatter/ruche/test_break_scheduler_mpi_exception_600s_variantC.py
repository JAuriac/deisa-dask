# Detects scheduler task scheduling limits under continuous submission

import argparse
import numpy as np
import os
import time
from mpi4py import MPI
from dask.distributed import Client, TimeoutError
from tornado.iostream import StreamClosedError
import asyncio
import dask

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

            future = client.submit(lambda x: x, data)

            end = time.perf_counter_ns()
            latencies.append(end - start)

            client.close()

        except TimeoutError as e:
            print("Connection or RPC timeout:", e)

        except StreamClosedError as e:
            print("Network stream closed (scheduler likely unreachable):", e)

        except asyncio.TimeoutError as e:
            print("Async operation timeout (event loop blocked):", e)

        except OSError as e:
            print("Low-level network error:", e)

        except Exception as e:
            print("Other error:", type(e), e)

            # optionally re-raise for debugging
            # raise

    if latencies:
        mean_latency = float(np.mean(latencies))
        success_count = len(latencies)
    else:
        mean_latency = np.nan
        success_count = 0

    os.makedirs("resExceptionVariantC", exist_ok=True)

    out_path = (
        f"resExceptionVariantC/scatter_times_"
        f"{size}_{args.data_size}_None_{rank}.csv"
    )

    np.savetxt(out_path,
               [[mean_latency, success_count]],
               delimiter=",")


if __name__ == "__main__":
#    print("worker-ttl =", dask.config.get("distributed.scheduler.worker-ttl"))
#    print("heartbeat interval =", dask.config.get("distributed.worker.heartbeat-interval"))
    main()
