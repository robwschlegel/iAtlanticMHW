#!/bin/bash

#SBATCH --job-name='iAtlanticMHW'
#SBATCH --cpus-per-task=48
#SBATCH --mem=250GB
#SBATCH --output=iAtlanticMHW-%j-stdout.log
#SBATCH --error=iAtlanticMHW-%j-stderr.log

echo "Submitting SLURM job"
cd iAtlanticMHW
singularity exec /software/containers/bionic-R3.6.2-RStudio1.2.5033.simg R code/functions.R