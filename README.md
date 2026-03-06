### Benchmark setup
Needs Python 3.10+ (can use 3.10 on G5K and 3.14 on Ruche as of 20-02-2026).

From anywhere: (example with Ruche)
```bash
git clone git@github.com:JAuriac/deisa-dask.git -b benchmark
module load python/3.14
module load openmpi/4.1.8/gcc-15.1.0
python3 -m venv ~/venv3.14_deisa-dask
source venv3.14_deisa-dask/bin/activate
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
With:
```bash
module load openmpi/4.1.8/gcc-15.1.0
source venv3.14_deisa-dask/bin/activate
```
then from benchmark/scatter/ruche:
```bash
sbatch ./controlled_latency_curve_mpi_variantA.sh
```
or:
```bash
sbatch ./break_test_mpi_exception_600s_variantA.sh
```
then:
```bash
sbatch slurm_plot_split_survival_unified_variants.sh
```
or directly:
```
python3 plot_split_survival_unified_variants.py
```
