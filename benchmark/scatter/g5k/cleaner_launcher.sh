#!/bin/bash

set -x

# Configuration du benchmark
SCHEFILE=~/deisa-dask/benchmark/scatter/g5k/scheduler.json
DASK_WORKER_NODES=1
DASK_NB_WORKERS=1
DASK_NB_THREAD_PER_WORKER=20
NB_CLIENTS=(4 8 16)
DATA_SIZES=(32 64 128)
NB_RUNS=5

# Nettoyage complet (ce qui est lié à Dask)
cleanup_all_dask() {
    local node=$1
    oarsh $node "
        # Tuer tous les processus Python liés à Dask
        pkill -f 'dask-scheduler' || true
        pkill -f 'dask-worker' || true
        pkill -f 'distributed.scheduler' || true
        pkill -f 'distributed.worker' || true
        pkill -f 'connect-clients.py' || true
        pkill -f 'dask.' || true
        pkill -f 'distributed.' || true

        # Libérer tous les ports 8786-8788 et 30000-40000
        for port in 8786 8787 8788; do
            PID=\$(lsof -ti :\$port 2>/dev/null)
            [ -n \"\${PID}\" ] && kill -9 \${PID} || true
        done

        # Attendre que les ports se libèrent
        sleep 3
    "
}

# Récupérer la liste des nœuds alloués par OAR
OARNODES=$(cat $OAR_FILE_NODES | uniq)
SCHEDULER_NODE=$(echo "$OARNODES" | head -n 1)
WORKER_NODES=$(echo "$OARNODES" | tail -n +2 | head -n $DASK_WORKER_NODES)
CLIENT_NODES=$(echo "$OARNODES" | tail -n +$((DASK_WORKER_NODES + 1)))

echo "Scheduler node: $SCHEDULER_NODE"
echo "Worker nodes: $WORKER_NODES"
echo "Client nodes: $CLIENT_NODES"

# Nettoyer TOUS les nœuds
for node in $OARNODES; do
    echo "Nettoyage complet du nœud $node..."
    cleanup_all_dask $node
done

# Trouver un port libre pour le scheduler (plage large, 49152-65535)
DASK_SCHEDULER_PORT=$(oarsh $SCHEDULER_NODE "
    for port in \$(seq 49152 65535 | shuf | head -n 1); do
        if ! nc -z localhost \$port 2>/dev/null; then
            echo \$port
            exit
        fi
    done
    echo \"ERREUR: Aucun port libre trouvé\" >&2
    exit 1
")

# Créer le dossier pour le fichier scheduler.json
oarsh $SCHEDULER_NODE "mkdir -p ~/deisa-dask/benchmark/scatter/g5k"

# Lancer le scheduler avec port explicite
echo "Lancement du scheduler sur $SCHEDULER_NODE:$DASK_SCHEDULER_PORT"
oarsh $SCHEDULER_NODE "
    source ~/venv3.10_deisa-dask/bin/activate &&
    dask scheduler \
        --port $DASK_SCHEDULER_PORT \
        --scheduler-file=$SCHEFILE \
        --no-dashboard
" > scheduler.log 2>&1 &
dask_sch_pid=$!

# Vérifier que le scheduler est prêt
MAX_WAIT=30
while true; do
    if oarsh $SCHEDULER_NODE "[ -f $SCHEFILE ]" && \
       oarsh $SCHEDULER_NODE "nc -z localhost $DASK_SCHEDULER_PORT" 2>/dev/null; then
        break
    fi
    sleep 1
    MAX_WAIT=$((MAX_WAIT-1))
    if [ $MAX_WAIT -le 0 ]; then
        echo "ERREUR: Timeout en attendant le scheduler"
        kill -9 $dask_sch_pid 2>/dev/null
        exit 1
    fi
    echo -n .
done

echo "Scheduler démarré avec succès !"
oarsh $SCHEDULER_NODE "cat $SCHEFILE"

# Lancer les workers sur les nœuds dédiés
dask_worker_pids=()
for node in $WORKER_NODES; do
    oarsh $node "
        source ~/venv3.10_deisa-dask/bin/activate &&
        dask worker \
            --nworkers ${DASK_NB_WORKERS} \
            --nthreads ${DASK_NB_THREAD_PER_WORKER} \
            --local-directory /tmp \
            --scheduler-file=${SCHEFILE} \
            --name worker-${node}
    " &
    dask_worker_pids+=($!)
done

sleep 5  # Attendre que les workers soient prêts

# Exécuter les clients
for data_size in "${DATA_SIZES[@]}"; do
    for nb_clients in "${NB_CLIENTS[@]}"; do
        for run_id in $(seq 1 ${NB_RUNS}); do
            pids=()
            id=1
            clients_per_node=$((nb_clients / $(echo "$CLIENT_NODES" | wc -w)))

            for client_node in $CLIENT_NODES; do
                echo "Lancement de $clients_per_node clients sur $client_node (IDs $id à $((id + clients_per_node - 1)))"
                for ((i=0; i<clients_per_node; i++)); do
                    if [ $id -le $nb_clients ]; then
                        oarsh $client_node "
                            source ~/venv3.10_deisa-dask/bin/activate &&
                            python3 ~/deisa-dask/benchmark/scatter/g5k/connect-clients.py \
                                ${SCHEFILE} \
                                ${nb_clients} \
                                ${data_size} \
                                ${run_id} \
                                ${id}
                        " &
                        pids+=($!)
                        id=$((id+1))
                    fi
                done
            done

            # Attendre la fin des clients
            for pid in "${pids[@]}"; do
                wait $pid || echo "Un client a échoué (PID: $pid)"
            done

            sleep 5
        done
    done
done

# Nettoyage final
echo "Nettoyage final..."
for pid in "${dask_worker_pids[@]}"; do
    kill -9 $pid 2>/dev/null || true
done
kill -9 ${dask_sch_pid} 2>/dev/null || true

# Nettoyage sur tous les nœuds
for node in $OARNODES; do
    cleanup_all_dask $node
    oarsh $node "rm -f ${SCHEFILE}" || true
done

echo "Benchmark terminé avec succès"
