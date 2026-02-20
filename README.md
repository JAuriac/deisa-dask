### Benchmark setup
Needs Python 3.10+ (can use 3.10 on G5K and 3.14 on Ruche as of 20-02-2026).

From anywhere:
```bash
git clone git@github.com:JAuriac/deisa-dask.git -b benchmark
module load python/3.10
python3 -m venv ~/venv3.10_deisa-dask
source venv3.10_deisa-dask/bin/activate
cd deisa-dask
pip install -e .
pip install numpy
```

### Benchmark run on G5K
From benchmark/scatter/g5k:
```bash
oarsub -n "deisa_scaling" -l "nodes=1,core=20,walltime=00:20:00" -S "./launcher.sh"
oarsub -n "deisa_scaling" -l "nodes=4,core=20,walltime=00:20:00" -S "./launcher.sh"
```

### Benchmark run on ruche
From benchmark/scatter/ruche:
```bash
sbatch ./launcher.sh
```
