#!/bin/bash
#SBATCH --job-name=dask_controlled_latency_curve
#SBATCH --output=%x_%j.out
#SBATCH --time=01:00:00
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=40
#SBATCH --cpus-per-task=1
#SBATCH --threads-per-core=1
#SBATCH --partition=cpu_short

export OPENBLAS_NUM_THREADS=1
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
ulimit -n 200000

# -----------------------
# Configuration
# -----------------------

SCHEFILE=$PWD/scheduler_latency.json
rm -f $SCHEFILE

DASK_WORKER_NODES=1
DASK_NB_WORKERS=20
DASK_NB_THREAD_PER_WORKER=1

#DATA_SIZES=(32 64 128 256)
#CLIENTS_LIST=(256 512 1024 2048 4096 8192)
DATA_SIZES=(32)
CLIENTS_LIST=(120)
NB_RUNS=3

# -----------------------
# Launch Scheduler
# -----------------------

echo "Launching Scheduler..."
srun -N 1 -n 1 -c 1 \
bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
         dask scheduler --scheduler-file=$SCHEFILE --no-dashboard" &
SCH_PID=$!
#         dask scheduler --scheduler-file=$SCHEFILE --no-dashboard" &

echo "Waiting for scheduler..."
while [ ! -f $SCHEFILE ]; do sleep 1; done
echo "Scheduler ready."

# -----------------------
# Launch Workers
# -----------------------

echo "Launching Workers..."
#srun -N $DASK_WORKER_NODES -n $DASK_WORKER_NODES -c 1 \
srun -N $DASK_WORKER_NODES -n 20 -c 1 \
bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
         dask worker --scheduler-file=$SCHEFILE \
         --nthreads ${DASK_NB_THREAD_PER_WORKER} \
         --no-dashboard" &
WORKER_PID=$!
#         --nworkers ${DASK_NB_WORKERS} \

sleep 5

# -----------------------
# CLIENT STRESS LOOP
# -----------------------

for data_size in "${DATA_SIZES[@]}"; do
  for nb_clients in "${CLIENTS_LIST[@]}"; do
    for run_id in $(seq 1 ${NB_RUNS}); do

      echo "========================================"
      echo "Data size: $data_size | Clients: $nb_clients | Run: $run_id"
      echo "========================================"

      # Launch all clients inside ONE srun
#      srun -N 1 -n 1 -c 40 bash -c "
#        source ~/venv3.14_deisa-dask/bin/activate
#        python3 test_latency_curve.py \
#            --scheduler-file $SCHEFILE \
#            --nb-clients $nb_clients \
#            --data-size $data_size \
#            --run-id $run_id
#      "

#      srun -N $SLURM_JOB_NUM_NODES -n $nb_clients -c 1 \
#      srun -N $nb_clients/40 -n $nb_clients -c 1 \
      srun -N 3 -n $nb_clients -c 1 \
          bash -c "source ~/venv3.14_deisa-dask/bin/activate && \
                   python3 test_latency_curve_mpi.py \
                       --scheduler-file $SCHEFILE \
                       --data-size $data_size \
                       --run-id $run_id"

      echo "Run finished"
      sleep 5

    done
  done
done

# -----------------------
# Cleanup
# -----------------------

echo "Cleaning up..."
kill -9 $WORKER_PID $SCH_PID
rm -f $SCHEFILE

echo "DONE"
