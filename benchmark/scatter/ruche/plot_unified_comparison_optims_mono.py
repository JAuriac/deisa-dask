import glob
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def load_data(res_dir):

    csv_files = sorted(
        glob.glob(os.path.join(res_dir, "scatter_times_*_*_*.csv"))
    )

    latency_data = defaultdict(list)  # (nc, run) -> [lat]

    for csv_file in csv_files:

        basename = os.path.basename(csv_file)
        stem = basename.removeprefix("scatter_times_").removesuffix(".csv")
        parts = stem.split("_")

        if len(parts) != 3:
            continue

        nb_clients, run_id, unique_id = parts

        try:
            nb_clients = int(nb_clients)
            run_id = int(run_id)
            unique_id = int(unique_id)
        except ValueError:
            continue

        arr = np.loadtxt(csv_file, delimiter=",")
        val = float(np.atleast_1d(arr)[0])

        if not np.isnan(val):
            latency_data[(nb_clients, run_id)].append(val)

    return latency_data


# ---------------------------
# LATENCY CURVE
# ---------------------------

def plot_latency_curve(latency_data, linestyle="-", label_suffix=""):

    by_nc = defaultdict(list)

    for (nc, run), vals in latency_data.items():
        run_mean = np.mean(vals)
        by_nc[nc].append(run_mean)

    if not by_nc:
        print("No latency test data found.")
        return

    clients = sorted(by_nc.keys())
    clients = [c for c in clients if c != 1]  # optional filter

    means = []
    stds = []

    for nc in clients:
        vals = np.asarray(by_nc[nc])
        means.append(vals.mean())
        stds.append(vals.std(ddof=1) if len(vals) > 1 else 0.0)

    plt.errorbar(
        clients,
        means,
        yerr=stds,
        marker="o",
        linestyle=linestyle,
        label=f"latency {label_suffix}",
    )



# ---------------------------

if __name__ == "__main__":

#    res1 = "~/deisa-dask/benchmark/scatter/ruche/resRelease3Runs2"
#    res2 = "~/DeisaDaskBis/deisa-dask/benchmark/scatter/ruche/res"
#    res1 = os.path.expanduser("~/deisa-dask/benchmark/scatter/ruche/resRelease3Runs")
    res1 = os.path.expanduser("~/deisa-dask/benchmark/scatter/ruche/res")
    res2 = os.path.expanduser("~/DeisaDaskBis/deisa-dask/benchmark/scatter/ruche/res")
    res3 = os.path.expanduser("~/DeisaDaskTer/deisa-dask/benchmark/scatter/ruche/res")

    # latency_data_1, _, _ = load_data(res1)
    # latency_data_2, _, _ = load_data(res2)
    # latency_data_3, _, _ = load_data(res3)

    # print("Latency data sizes Default:", sorted({k[0] for k in latency_data_1}))
    # print("Latency data sizes Optim 1:", sorted({k[0] for k in latency_data_2}))
    # print("Latency data sizes Optim 2:", sorted({k[0] for k in latency_data_3}))

    latency_data_1 = load_data(res1)
    latency_data_2 = load_data(res2)
    latency_data_3 = load_data(res3)

    print("Clients Default:", sorted({k[0] for k in latency_data_1}))
    print("Clients Optim 1:", sorted({k[0] for k in latency_data_2}))
    print("Clients Optim 2:", sorted({k[0] for k in latency_data_3}))

    plt.figure()

    plot_latency_curve(latency_data_1, linestyle="-", label_suffix="(Default)")
    plot_latency_curve(latency_data_2, linestyle="--", label_suffix="(Optim 1)")
    plot_latency_curve(latency_data_3, linestyle=":", label_suffix="(Optim 2)")

    plt.yscale("log")
    plt.xlabel("Number of clients")
    plt.ylabel("Scatter latency (ns)")
    plt.title("Burst latency comparison")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()

    plt.savefig("plot_latency_curve_compare_optims_mono.png")
    plt.close()

    print("Saved plot_latency_curve_compare_optims_mono.png")
