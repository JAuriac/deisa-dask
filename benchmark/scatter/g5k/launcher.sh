#!/bin/bash

set -x

SCHEFILE=~/deisa-dask/benchmark/scatter/g5k/scheduler.json
DASK_WORKER_NODES=1
DASK_NB_WORKERS=1
DASK_NB_THREAD_PER_WORKER=20  # Ajusté pour correspondre à --cpus-per-task=20
#NB_CLIENTS=(8 16 32)
NB_CLIENTS=(4 8 16)
#DATA_SIZES=(64 128 256)
DATA_SIZES=(32 64 128)
NB_RUNS=5

# Récupérer la liste des nœuds alloués par OAR
OARNODES=$(cat $OAR_FILE_NODES | uniq)
SCHEDULER_NODE=$(echo "$OARNODES" | head -n 1)
WORKER_NODES=$(echo "$OARNODES" | tail -n +2 | head -n $DASK_WORKER_NODES)
CLIENT_NODES=$(echo "$OARNODES" | tail -n +$((DASK_WORKER_NODES + 1)))

echo "Scheduler node: $SCHEDULER_NODE"
echo "Worker nodes: $WORKER_NODES"
echo "Client nodes: $CLIENT_NODES"

# Lancer le scheduler sur le premier nœud
echo "Launching Dask Scheduler on $SCHEDULER_NODE"
oarsh $SCHEDULER_NODE "source ~/venv3.10_deisa-dask/bin/activate && dask scheduler --scheduler-file=$SCHEFILE" &
dask_sch_pid=$!

# Attendre que le fichier scheduler soit créé
while ! [ -f $SCHEFILE ]; do
    sleep 1
    echo -n .
done
echo "Scheduler booted, launching workers"

# Lancer les workers sur les nœuds dédiés
for node in $WORKER_NODES; do
    oarsh $node "source ~/venv3.10_deisa-dask/bin/activate && dask worker \
        --nworkers ${DASK_NB_WORKERS} \
        --nthreads ${DASK_NB_THREAD_PER_WORKER} \
        --local-directory /tmp \
        --scheduler-file=${SCHEFILE}" &
    dask_worker_pids+=($!)
done

sleep 1

# Exécuter les clients
for data_size in "${DATA_SIZES[@]}"; do
    for nb_clients in "${NB_CLIENTS[@]}"; do
        for run_id in $(seq 1 ${NB_RUNS}); do
            id=1
            clients_per_node=$((nb_clients / $(echo "$CLIENT_NODES" | wc -w)))
            for client_node in $CLIENT_NODES; do
                echo "Running $clients_per_node clients on node $client_node (id=$id to $((id + clients_per_node - 1)))"
                for ((i=0; i<clients_per_node; i++)); do
                    if [ $id -le $nb_clients ]; then
                        oarsh $client_node "source ~/venv3.10_deisa-dask/bin/activate && python3 ~/deisa-dask/benchmark/scatter/g5k/connect-clients.py $SCHEFILE \"${nb_clients}\" \"${data_size}\" \"${run_id}\" \"${id}\"" &
                        pids+=($!)
                        id=$((id+1))
                    fi
                done
            done
            wait "${pids[@]}"
            pids=()
            sleep 5
        done
    done
done

# Nettoyage
for pid in "${dask_worker_pids[@]}"; do
    kill -9 $pid 2>/dev/null
done
kill -9 ${dask_sch_pid} 2>/dev/null
rm -f $SCHEFILE

echo "Done"
