# Running the Scheduler on Compute Nodes

On public supercomputers, jobs running on login nodes usually have limited resources and wall time. The Rust scheduler of sdag is intentionally lightweight so it can typically survive many hours on login nodes without being killed. If the SSH connection goes down, `sdag restart` can be used to safely restart the scheduler and pick the execution from where it left off.

Nevertheless, for long workflows across large graphs it may be better to just submit the scheduler as a Slurm job on a compute node. Copy the following script into a `submit_scheduler.sh` file:

```sh  title="submit_scheduler.sh"
#!/bin/bash
#SBATCH --job-name=scheduler
#SBATCH --account=<account-name>
#SBATCH --partition=<partition-name>
#SBATCH --qos=<qos-name>
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=1-00:00:00
#SBATCH --output ./logs/%x.%j.out
#SBATCH --error ./logs/%x.%j.err

source .venv/bin/activate
sdag $@
```

For example, to compile and run a pipeline you can execute:

```sh
sbatch submit_scheduler.sh run <pipeline-name>
```