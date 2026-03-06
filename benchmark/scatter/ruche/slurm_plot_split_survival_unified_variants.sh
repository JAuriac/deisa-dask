#!/bin/bash
#SBATCH --job-name=plotter_variants
#SBATCH --output=%x_%j.out
#SBATCH --time=01:00:00
#SBATCH --nodes=1
##SBATCH --ntasks-per-node=40
##SBATCH --cpus-per-task=1
##SBATCH --threads-per-core=1
#SBATCH --partition=cpu_short

source ~/venv3.14_deisa-dask/bin/activate
python3 plot_split_survival_unified_variants.py
