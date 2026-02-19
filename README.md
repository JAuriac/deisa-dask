### Benchmark setup
```bash
git clone git@github.com:JAuriac/deisa-dask.git -b benchmark
module load python/3.10
python3 -m venv ~/venv3.10_deisa-dask
source venv3.10_deisa-dask/bin/activate
pip install -e .
pip install pytest
pip install numpy
```

### Benchmark run
```bash
oarsub -n "deisa_scaling" -l "nodes=1,core=20,walltime=00:20:00" -S "./launcher.sh"
oarsub -n "deisa_scaling" -l "nodes=4,core=20,walltime=00:20:00" -S "./launcher.sh"
```
