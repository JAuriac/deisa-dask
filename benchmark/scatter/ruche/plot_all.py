import glob
import os
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


BREAK_VARIANTS = ["A", "B", "C", "D"]
BURST_VARIANTS = ["A", "B", "C", "D"]
LINESTYLES = {
    "A": "-",
    "B": "--",
    "C": ":",
    "D": "-."
}


# ------------------------------------------------
# LOAD DATA
# ------------------------------------------------

def load_data():

    connection_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # variant -> data_size -> clients -> [rate]

    burst_latency = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # variant -> data_size -> clients -> [latency]

    # -----------------------------
    # BREAK TEST (connection rate)
    # -----------------------------

    for variant in BREAK_VARIANTS:

        path = f"resExceptionVariant{variant}"

#        files = glob.glob(os.path.join(path, "connection_rate_*_*_*_*.csv"))
#
#        for f in files:
#
#            name = os.path.basename(f)
#            stem = name.removeprefix("connection_rate_").removesuffix(".csv")
#            parts = stem.split("_")

        files = glob.glob(os.path.join(path, "scatter_times_*_*_*_*.csv"))

        print("Loading break variant", variant, "files:", len(files))

        for f in files:

            name = os.path.basename(f)
            stem = name.removeprefix("scatter_times_").removesuffix(".csv")
            parts = stem.split("_")

            if len(parts) != 4:
                continue

            nb_clients, data_size, run_id, rank = parts

            try:
                nb_clients = int(nb_clients)
                data_size = int(data_size)
            except ValueError:
                continue

#            arr = np.loadtxt(f, delimiter=",").flatten()
#
#            if len(arr) != 7:
#                continue
#
#            rate = arr[0]

            with open(f) as fh:
                line = fh.readline().strip()

            try:
                rate = float(line.split(",")[0])
            except ValueError:
                continue

            connection_data[variant][data_size][nb_clients].append(rate)

    # -----------------------------
    # BURST TEST (latency)
    # -----------------------------

    for variant in BURST_VARIANTS:

        path = f"resBurstVariant{variant}"

        files = glob.glob(os.path.join(path, "scatter_times_*_*_*_*.csv"))

        print("Loading burst variant", variant, "files:", len(files))

        for f in files:

            name = os.path.basename(f)
            stem = name.removeprefix("scatter_times_").removesuffix(".csv")
            parts = stem.split("_")

            if len(parts) != 4:
                continue

            nb_clients, data_size, run_id, rank = parts

            try:
                nb_clients = int(nb_clients)
                data_size = int(data_size)
            except ValueError:
                continue

#            arr = np.loadtxt(f, delimiter=",")
#            arr = np.atleast_1d(arr)
#
#            val = float(arr[0])

            with open(f) as fh:
                line = fh.readline().strip()

            try:
                val = float(line)
            except ValueError:
                continue

            if not np.isnan(val):
                burst_latency[variant][data_size][nb_clients].append(val)

    return connection_data, burst_latency


# ------------------------------------------------
# BREAK TEST PLOT (CONNECTION RATE)
# ------------------------------------------------

def plot_connection_rate(connection_data):

    plt.figure()

    colors = plt.cm.tab10.colors

#    for variant in sorted(connection_data.keys()):
#
#        linestyle = LINESTYLES.get(variant, "-")
#
#        for j, ds in enumerate(sorted(connection_data[variant].keys())):
#
#            clients = sorted(connection_data[variant][ds].keys())
#
#            means = []
#
#            for nc in clients:
#                vals = np.asarray(connection_data[variant][ds][nc])
#                means.append(vals.mean())
#
#            plt.plot(
#                clients,
#                means,
#                marker="o",
#                linestyle=linestyle,
#                color=colors[j % len(colors)],
#                label=f"Variant {variant}, ds={ds}"
#            )

    for variant in sorted(connection_data.keys()):

        linestyle = LINESTYLES.get(variant, "-")

        if variant == "D":
#            ds_list = [sorted(connection_data[variant].keys())[0]]
            ds_list = [min(connection_data[variant].keys())]
        else:
            ds_list = sorted(connection_data[variant].keys())

        for j, ds in enumerate(ds_list):

            clients = sorted(connection_data[variant][ds].keys())

            means = []

            for nc in clients:
                vals = np.asarray(connection_data[variant][ds][nc])
                means.append(vals.mean())

            label = f"Variant {variant}" if variant == "D" else f"Variant {variant}, ds={ds}"

            plt.plot(
                clients,
                means,
                marker="o",
                linestyle=linestyle,
                color=colors[j % len(colors)],
                label=label
            )

    plt.xlabel("Number of clients")
    plt.ylabel("Connection rate (ops/sec)")
    plt.title("Scheduler connection throughput")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plot_connection_rate_all.png")
    plt.close()

    print("Saved plot_connection_rate_all.png")


# ------------------------------------------------
# BURST LATENCY PLOT
# ------------------------------------------------

def plot_burst_latency(burst_latency):

    plt.figure()

    colors = plt.cm.tab10.colors

#    for i, variant in enumerate(sorted(burst_latency.keys())):
#
#        linestyle = LINESTYLES.get(variant, "-")
#
#        for j, ds in enumerate(sorted(burst_latency[variant].keys())):
#
#            clients = sorted(burst_latency[variant][ds].keys())
#
#            means = []
#            stds = []
#
#            for nc in clients:
#                vals = np.asarray(burst_latency[variant][ds][nc])
#                means.append(vals.mean())
#                stds.append(vals.std(ddof=1) if len(vals) > 1 else 0)
#
#            plt.errorbar(
#                clients,
#                means,
#                yerr=stds,
#                color=colors[j % len(colors)],
#                linestyle=linestyle,
#                marker="o",
#                label=f"Variant {variant}, ds={ds}"
#            )
    for variant in sorted(burst_latency.keys()):

        linestyle = LINESTYLES.get(variant, "-")

        if variant == "D":
#            ds_list = [sorted(burst_latency[variant].keys())[0]]
            ds_list = [min(burst_latency[variant].keys())]
        else:
            ds_list = sorted(burst_latency[variant].keys())

        for j, ds in enumerate(ds_list):

            clients = sorted(burst_latency[variant][ds].keys())

            means = []
            stds = []

            for nc in clients:
                vals = np.asarray(burst_latency[variant][ds][nc])
                means.append(vals.mean())
                stds.append(vals.std(ddof=1) if len(vals) > 1 else 0)

            label = f"Variant {variant}" if variant == "D" else f"Variant {variant}, ds={ds}"

            plt.errorbar(
                clients,
                means,
                yerr=stds,
                linestyle=linestyle,
                marker="o",
                label=label
            )

    plt.yscale("log")
    plt.xlabel("Number of clients")
    plt.ylabel("Scatter latency (ns)")
    plt.title("Scheduler burst latency")
    plt.grid(True, which="both", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig("plot_burst_latency_all.png")
    plt.close()

    print("Saved plot_burst_latency_all.png")


# ------------------------------------------------
# MAIN
# ------------------------------------------------

if __name__ == "__main__":

    connection_data, burst_latency = load_data()

    plot_connection_rate(connection_data)
    plot_burst_latency(burst_latency)
