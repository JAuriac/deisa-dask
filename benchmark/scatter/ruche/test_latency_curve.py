import argparse
import multiprocessing as mp
import numpy as np
import os
import time
from dask.distributed import Client


def worker(cid, scheduler_file, data_size,
           nb_clients, run_id,
           ready_barrier, fire_barrier):

    client = Client(scheduler_file=scheduler_file)

    data = np.ones((data_size, data_size, data_size), dtype=np.float64)

    ready_barrier.wait()
    fire_barrier.wait()

    start = time.perf_counter_ns()

    future = client.scatter(data, broadcast=True)
    client.gather(future)

    end = time.perf_counter_ns()

    latency_ns = end - start

    os.makedirs("res", exist_ok=True)

    out_path = (
        f"res/scatter_times_"
        f"{nb_clients}_{data_size}_{run_id}_{cid}.csv"
    )

    np.savetxt(out_path, [latency_ns], delimiter=",")

    client.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-file")
    parser.add_argument("--nb-clients", type=int)
    parser.add_argument("--data-size", type=int)
    parser.add_argument("--run-id", type=int)
    args = parser.parse_args()

    mp.set_start_method("fork", force=True)

    ready_barrier = mp.Barrier(args.nb_clients)
    fire_barrier = mp.Barrier(args.nb_clients)

    procs = []

    for i in range(args.nb_clients):
        p = mp.Process(
            target=worker,
            args=(i,
                  args.scheduler_file,
                  args.data_size,
                  args.nb_clients,
                  args.run_id,
                  ready_barrier,
                  fire_barrier)
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join()
