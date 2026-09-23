# Failure Recovery

!!! info
    To run these examples with Slurm, turn `script.sh` into a [sbatch](https://slurm.schedmd.com/sbatch.html) script as explained in the [Hello World](00_hello_world.md) section and execute pipelines without the `--local` option.

Failures are unfortunately very common in HPC. In the context of sdag, the two most common failures that can happen are:

1. The sdag scheduler gets killed
2. Tasks fail for whatever reason

## Scheduler failures

The scheduler checkpoints its status. So, in case of failures it can be restarted from where it left off. To restart the scheduler, run:

```sh
sdag restart <pipeline-name>
```

Once restarted, it get from Slurm the status of the supposedly running jobs to update the graph state. However, all previously running local jobs (that get killed if the scheduler goes down) will be marked as failed.

## Task failures

Create the following two files in the main project directory:

`script.sh` (if missing):

```sh
sdag-execute
```

`failure_recovery.py`:

```py
from sdag import pipeline


@pipeline
def failure():
    t = failed_task()
    will_never_run(t)


@failure.task("script.sh")
def failed_task():
    raise ValueError("Failing...")


@failure.task("script.sh")
def will_never_run():
    print("You will never see this message")
```

Run:

```sh
sdag run failure --local
```

to execute the pipeline. As expected, `failed_task` will fail and all dependent nodes will be skipped. If you fix the bug and the graph structure doesn't change, you can retry the run and see if it completes successfully:

```sh
sdag retry failure
```

`retry` will reset all failed and skipped task before continuing the run. If no task failed or get skipped, then `sdag retry` it's virtually identical to `sdag restart` of the previous section. You can also set the number of retries for a task. Modify `failed_task` as follows:

```py
@failure.task("script.sh", retries=2)
def failed_task():
    raise ValueError("Failing...")
```

And run the pipeline again:

```sh
sdag run failure --local
```

You will see that the execution of `failed_task` is retried two times before marking it as failed definitively.
