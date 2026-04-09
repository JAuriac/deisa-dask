#!/bin/bash
#SBATCH --job-name=dask_controlled_latency_curve
#SBATCH --output=%x_%j.out
#SBATCH --time=04:00:00
#SBATCH --nodes=14
#SBATCH --ntasks-per-node=40
#SBATCH --cpus-per-task=1
#SBATCH --threads-per-core=1
#SBATCH --partition=cpu_med

export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
ulimit -n 200000

module load openmpi/4.1.8/gcc-15.1.0

# -----------------------
# Configuration
# -----------------------

SCHEFILE=$PWD/scheduler_burst_latency.json
rm -f $SCHEFILE

DASK_WORKER_NODES=1
DASK_NB_WORKERS=40
DASK_NB_THREAD_PER_WORKER=1

CLIENTS_LIST=(1 2 5 10 20 40 80 120 160 200 240 360 480)
NB_RUNS=5

# -----------------------
# Launch Scheduler
# -----------------------

echo "Launching Scheduler..."
srun -N 1 -n 1 -c 1 \
bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
         dask scheduler --scheduler-file=$SCHEFILE --no-dashboard" &
SCH_PID=$!

while [ ! -f $SCHEFILE ]; do sleep 1; done
echo "Scheduler ready."

# -----------------------
# Launch Workers
# -----------------------

echo "Launching Workers..."
srun -N $DASK_WORKER_NODES -n $DASK_NB_WORKERS -c 1 \
bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
         dask worker --scheduler-file=$SCHEFILE \
         --nthreads ${DASK_NB_THREAD_PER_WORKER} \
         --no-dashboard" &
WORKER_PID=$!

sleep 5

# -----------------------
# CLIENT LOOP
# -----------------------

for nb_clients in "${CLIENTS_LIST[@]}"; do
  for run_id in $(seq 1 ${NB_RUNS}); do

    echo "========================================"
    echo "Clients: $nb_clients | Run: $run_id"
    echo "========================================"

    # We allocate one node per forty clients, as a node on ruche has forty CPUs.
    # If less than forty are needed, we round up to use one node.
    NODES_CLIENTS=$(( (nb_clients + 39) / 40 ))

    srun -N ${NODES_CLIENTS} -n ${nb_clients} -c 1 \
      bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
               python3 test_burst_controlled_latency_mono.py \
                   --scheduler-file ${SCHEFILE} \
                   --run-id ${run_id}"

    echo "Run finished"
    sleep 5

  done
done

# -----------------------
# Cleanup
# -----------------------

echo "Cleaning up..."
kill -9 $WORKER_PID $SCH_PID
rm -f $SCHEFILE

echo "DONE"
