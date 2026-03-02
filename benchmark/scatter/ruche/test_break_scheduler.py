import argparse
import multiprocessing as mp
import numpy as np
import os
import time
from dask.distributed import Client


def hammer(cid, scheduler_file,
           data_size, nb_clients,
           run_id, duration):

    stop_time = time.time() + duration
    latencies = []

    data = np.ones((data_size, data_size, data_size), dtype=np.float64)

    while time.time() < stop_time:
        try:
            client = Client(scheduler_file=scheduler_file, timeout="2s")

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
        f"{nb_clients}_{data_size}_{run_id}_{cid}.csv"
    )

#    np.savetxt(out_path, [mean_latency], delimiter=",")
    np.savetxt(out_path,
           [[mean_latency, success_count]],
           delimiter=",")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-file")
    parser.add_argument("--nb-clients", type=int)
    parser.add_argument("--data-size", type=int)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--duration", type=int)
    args = parser.parse_args()

    mp.set_start_method("fork", force=True)

    procs = []

    for i in range(args.nb_clients):
        p = mp.Process(
            target=hammer,
            args=(i,
                  args.scheduler_file,
                  args.data_size,
                  args.nb_clients,
                  args.run_id,
                  args.duration)
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join()
