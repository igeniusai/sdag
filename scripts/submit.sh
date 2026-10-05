#!/bin/bash
#SBATCH --account=<account>
#SBATCH --partition=<partition>
#SBATCH --qos=<qos>
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:01:00

sdag-execute
