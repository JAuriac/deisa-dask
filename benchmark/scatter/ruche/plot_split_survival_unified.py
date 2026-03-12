import glob
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def load_data():

    csv_files = sorted(
        glob.glob(os.path.join("res", "scatter_times_*_*_*_*.csv"))
    )

    latency_data = defaultdict(list)  # (ds,nc,run) -> [lat]
    break_latency = defaultdict(lambda: defaultdict(list))  # ds->nc->[lat]
    break_success = defaultdict(lambda: defaultdict(list))  # ds->nc->[count]

    for csv_file in csv_files:

        basename = os.path.basename(csv_file)
        stem = basename.removeprefix("scatter_times_").removesuffix(".csv")
        parts = stem.split("_")
        if len(parts) != 4:
            continue

        nb_clients, data_size, run_id, unique_id = parts

        try:
            nb_clients = int(nb_clients)
            data_size = int(data_size)
            unique_id = int(unique_id)
        except ValueError:
            continue

        arr = np.loadtxt(csv_file, delimiter=",")
        arr = np.atleast_1d(arr)

        # LATENCY TEST (1 column)
        if run_id != "None":

            if arr.ndim == 1:
                val = float(arr[0])
            else:
                val = float(arr[0, 0])

            if not np.isnan(val):
                run_id = int(run_id)
                latency_data[(data_size, nb_clients, run_id)].append(val)

        # BREAK TEST (2 columns)
        else:

            arr = np.atleast_1d(arr).flatten()

            if len(arr) == 2:
                mean_latency = float(arr[0])
                success_count = float(arr[1])
            elif len(arr) == 1:
                mean_latency = float(arr[0])
                success_count = 0.0
            else:
                continue

            if not np.isnan(mean_latency):
                break_latency[data_size][nb_clients].append(mean_latency)
                break_success[data_size][nb_clients].append(success_count)

    return latency_data, break_latency, break_success


# ---------------------------
# BURST LATENCY PLOT
# ---------------------------

def plot_latency_curve(latency_data):

    by_ds_nc = defaultdict(lambda: defaultdict(list))

    for (ds, nc, run), vals in latency_data.items():
        run_mean = np.mean(vals)
        by_ds_nc[ds][nc].append(run_mean)

    if not by_ds_nc:
        print("No latency test data found.")
        return

    plt.figure()

    for ds in sorted(by_ds_nc.keys()):

        clients = sorted(by_ds_nc[ds].keys())
        means = []
        stds = []

        for nc in clients:
            vals = np.asarray(by_ds_nc[ds][nc])
            means.append(vals.mean())
            stds.append(vals.std(ddof=1) if len(vals) > 1 else 0.0)

        plt.errorbar(clients, means, yerr=stds, marker="o", label=f"data_size={ds}")

    plt.yscale("log")
    plt.xlabel("Number of clients")
    plt.ylabel("Scatter latency (ns)")
    plt.title("Burst latency curve")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plot_latency_curve_all.png")
    plt.close()

    print("Saved plot_latency_curve_all.png")


# ---------------------------
# BREAK TEST PLOTS
# ---------------------------

def plot_break_test(break_latency, break_success):

    # ---------- LATENCY UNDER CHURN ----------
    plt.figure()

    for ds in sorted(break_latency.keys()):

        clients = sorted(break_latency[ds].keys())
        lat_means = []
        lat_stds = []

        for nc in clients:
            vals = np.asarray(break_latency[ds][nc])
            lat_means.append(vals.mean())
            lat_stds.append(vals.std(ddof=1) if len(vals) > 1 else 0.0)

        plt.errorbar(clients, lat_means, yerr=lat_stds,
                     marker="o", label=f"data_size={ds}")

    plt.yscale("log")
    plt.xlabel("Number of clients")
    plt.ylabel("Mean scatter latency (ns)")
    plt.title("Break test latency")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plot_break_latency_all.png")
    plt.close()

    print("Saved plot_break_latency_all.png")

    # ---------- SURVIVAL CURVE ----------
    plt.figure()

    for ds in sorted(break_success.keys()):

        clients = sorted(break_success[ds].keys())
        success_means = []

        for nc in clients:
            vals = np.asarray(break_success[ds][nc])
            success_means.append(vals.mean())

        plt.plot(clients, success_means,
                 marker="o", label=f"data_size={ds}")

    plt.xlabel("Number of clients")
    plt.ylabel("Mean successful scatters per client")
    plt.title("Scheduler survival curve")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plot_survival_curve_all.png")
    plt.close()

    print(ds, nc, "std =", vals.std(ddof=1))
    print("Saved plot_survival_curve_all.png")


# ---------------------------

if __name__ == "__main__":

    latency_data, break_latency, break_success = load_data()

    print("Latency data sizes:", sorted({k[0] for k in latency_data}))
    print("Break data sizes:", sorted(break_latency.keys()))

    plot_latency_curve(latency_data)
    plot_break_test(break_latency, break_success)
