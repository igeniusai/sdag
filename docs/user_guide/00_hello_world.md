# Hello World in sdag

Create a new file named `hello.py` and paste the following snippet:

```py
from sdag import pipeline, Script


@pipeline
def hello_world():
    say_hello(name="sdag")


@hello_world.task(script=Script("sdag-execute"), cmd="bash")
def say_hello(name: str):
    print(f"hello from {name}")
```

There are two fundamental objects in sdag, **pipelines** and **tasks**. Tasks run as separate jobs and are scheduled depending on the current state. Pipelines (marked with the `@pipeline` decorator) are used to represent the structure of the graph that will be executed. The `script` argument of the task decorator contains the script that will be executed, which is simply `sdag-execute` in this example. Through this command sdag will dispatch the execution to the correct function. `cmd="bash"` is used to specify that the task must run as a nonblocking, local process instead of a Slurm job.

Differently from other libraries, in sdag you don't have specify a runner for the whole pipelines. Local and Slurm task can be freely mixed in the same workflow. This choice has been made to avoid spinning Slurm jobs that stay in queue forever for small operations that can be easily performed locally.

This pipeline can be compiled with:

```sh
sdag compile hello:hello_world
```

Where `hello:hello_world` is the pipeline import string in the form `path.to.module:function_name`.

!!! info
    It is often convenient to configure the base folder sdag searches pipelines into (which is `./pipelines` by default), so you don't have to specify the whole import string every time.

The previous command generates a `hello_world.json` file, which is saved in a `./compiled-pipelines` folder created by sdag by default. To run the compiled pipeline, execute:

```sh
sdag run hello_world.json
```

This command will start the scheduler and execute the `say_hello` task. You should see tables like these ones in the terminal (the task pid will likely be different):
```
    ┌─────┬───────────┬─────────────┬───────────────────────┬─────────┐
    │ uid │ task      │ pipeline    │ status                │ try_num │
    ├─────┼───────────┼─────────────┼───────────────────────┼─────────┤
    │   2 │ say_hello │ hello_world │ Completed (pid=25245) │ 1       │
    └─────┴───────────┴─────────────┴───────────────────────┴─────────┘
[2026-09-21T06:29:01Z INFO  core::engine::scheduler] Simulation completed
[2026-09-21T06:29:01Z INFO  core::engine::summary] Final recap:
    ┌───────────────┬────────┐
    │        status │ ntasks │
    ├───────────────┼────────┤
    │     Completed │ 1      │
    ├───────────────┼────────┤
    │       Skipped │ 0      │
    ├───────────────┼────────┤
    │        Failed │ 0      │
    ├───────────────┼────────┤
    │       Pending │ 0      │
    ├───────────────┼────────┤
    │       Running │ 0      │
    ├───────────────┼────────┤
    │ Not submitted │ 0      │
    └───────────────┴────────┘
```

Notice that in `sdag run` you didn't have to type the full `./compiled-pipelines/hello_world.json` path as by default it is automatically prepended. If you instead executed `sdag run hello:hello_world` (thus without pointing to a compiled JSON file), the `hello_world` pipeline would be compiled on the fly and then executed. Most sdag commands accept either pipeline names, import strings, or compiled JSON paths. You can check out the [CLI](09_cli.md) section for additional information. The latter allows one to skip the compilation step.

To run the same pipeline on a Slurm cluster, create a standard [sbatch](https://slurm.schedmd.com/sbatch.html) script named `script.sh` and paste the following script:

```sh
#!/bin/bash
#SBATCH --account=<account-name>
#SBATCH --partition=<partition-name>
#SBATCH --qos=<qos-name>
#SBATCH --nodes=1
#SBATCH --tasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:00:30

sdag-execute
```
!!!info
    fields like `job-name`, `output`, and `error` are intentionally missing here as sdag overrides them by default.

where `<account-name>`, `<partition-name>`, and `<qos-name>` are the Slurm account, partition, and quality of service you have access to. You can check out the [configurations](08_configurations.md) and [tags](../advanced/08_tags.md) sections to learn how to set these quantities globally or for groups of tasks. Now, modify `say_hello` as follows:

```py
@hello_world.task("script.sh")
def say_hello(name: str):
    print(f"hello from {name}")
```

As you can see, you can directly insert the script path into the decorator. This is how tasks are typically managed in non-trivial scenarios. You can now run the DAG on Slurm with the same command:

```sh
sdag run hello:hello_world
```

By default, task logs are written in a `logs/` folder created in the main project directory. For debugging purposes, you can still run all tasks locally through the cli:

```sh
sdag run hello:hello_world --local
```

`--local` will set `cmd="bash"` to all tasks, thus forcing them run as local processes.
