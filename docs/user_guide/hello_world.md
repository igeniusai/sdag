# Hello World in sdag

After [installing](../installation.md) sdag, create the following two files in the main project directory:


=== "`hello.py`"

    ```py
    from sdag import pipeline


    @pipeline
    def hello_world():
        say_hello(name="sdag")


    @hello_world.task("script.sh")
    def say_hello(name: str):
        print(f"hello from {name}")
    ```

=== "`script.sh`"

    ```sh
    sdag-execute
    ```

There are two fundamental objects in sdag, **pipelines** and **tasks**. Pipelines (marked with the `@pipeline` decorator) are used to represent the structure of the graph that will be executed. Tasks (marked with `@task` or `<pipeline-name>@task` decorators) are jobs scheduled depending on the current state of the graph.  The `script` argument of the task decorator contains the path to the script that will be fired by the scheduler, which is simply `sdag-execute` in this example. Through this command, sdag will dispatch the execution to the correct function. This pipeline can be compiled with:

```sh
sdag compile hello:hello_world
```

Where `hello:hello_world` is the pipeline import string in the form `path.to.module:function`. The previous command generates a `hello_world.json` file, which is saved in a `./compiled-pipelines` folder created by sdag. In many cases, you won't have to specify the full import string as sdag will be able to discover pipelines simply by their name:

```sh
sdag compile hello_world
```

!!! info
    In more structured codebases it is convenient to explicitly define the base directory pipelines are searched into. Check out the [Structuring Larger Projects](../advanced/larger_projects.md) section for details.

To run the compiled pipeline, execute:

```sh
sdag run hello_world.json --local
```

This command will start the scheduler and execute the `say_hello` task. You should see tables like this one printed in the terminal (although the task pid will likely be different):

```
    ┌─────┬───────────┬─────────────┬───────────────────────┬─────────┐
    │ uid │ task      │ pipeline    │ status                │ try_num │
    ├─────┼───────────┼─────────────┼───────────────────────┼─────────┤
    │   2 │ say_hello │ hello_world │ Completed (pid=25245) │ 1       │
    └─────┴───────────┴─────────────┴───────────────────────┴─────────┘
```

If you instead executed:

```sh
sdag run hello_world --local
```

(thus without pointing to a compiled JSON file), the `hello_world` pipeline would be compiled on the fly and then immediately executed. Several sdag commands accept either pipeline names, import strings, or compiled JSON paths. The latter allows one to skip the compilation step. You can check out the [CLI](../cli.md) section for additional information.

The `--local` option is used to run tasks locally as explained in the [local task](external_tasks.md) section. To run the same pipeline on a Slurm cluster, create a standard [sbatch](https://slurm.schedmd.com/sbatch.html) script by modifying `script.sh` as follows:

```sh title="script.sh"
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

where `<account-name>`, `<partition-name>`, and `<qos-name>` are the Slurm account, partition, and quality of service you have access to. You can check out the [Configurations](../configurations.md) and [Tags](../advanced/tags.md) sections to learn how to set these quantities globally or for groups of tasks. You can now run the DAG on the HPC with the following command:

```sh
sdag run hello_world
```

By default, task logs are written in a `logs/` folder created in the main project directory.
