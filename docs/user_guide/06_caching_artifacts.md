# Caching and Artifacts

!!! info
    To run these examples with Slurm, turn `script.sh` into a [sbatch](https://slurm.schedmd.com/sbatch.html) script as explained in the [Hello World](00_hello_world.md) section and execute pipelines without the `--local` option.

Caching is used to persist the output of a task across multiple runs, so you don't have to re-execute it again. Create the following two files in the main project directory:

`script.sh` (if missing):

```sh
sdag-execute
```

`caching.py`:

```py
from sdag import pipeline


@pipeline
def cached():
    t = cached_task(a="world")
    non_cached_task(t)


@cached.task("script.sh", cache=True)
def cached_task(a: str):
    print(f"I only run once. The value of a is {a}")


@cached.task("script.sh")
def non_cached_task():
    print("I run every time")
```

When this pipeline is executed the first time via `sdag run cached --local`, both tasks will be scheduled. When executed again, `cached_task` will be cached, so it will be marked as completed without execution. For caching to be validated, all input values must match with the cached ones. You can try to modify the value of `a` to check the cache gets invalidated. You can increase the `cache_size` in the decorator (which is equal to `1` by default) to account for multiple values of `a`. You can use the `cache_ignore` field of the task decorator to list the arguments you don't bother checking during cache validation. `cache_ignore` is useful for things like endpoints or parameters that are allowed to change without compromising cached values.

The cache is bound to a task name. Many times, instead of increasing the cache size you can change the task name to provide it a whole new cache. Copy the following snippet into the same module:

```py
@pipeline
def name_change():
    a_task(value=1)
    t = a_task(value=2)
    t.name = "another_task"


@name_change.task("script.sh", cache=True)
def a_task(value: int):
    print(f"value: {value}")
```

Because the two tasks have a different name, they both get cached during the first execution, which can be triggered with:

```sh
sdag run name_change --local
```

You also get the extra benefit of showing the two tasks with different names in the logs.

## Artifacts

Artifacts can be used to signal to sdag that a task must produce some kind of output (typically files or folders). Copy the following snippet into the same module:

```py
from pathlib import Path

from sdag import Artifact


@pipeline
def dag_with_artifact():
    t = create_file(path="artifact.txt")
    print_path(input_path=t.artifacts["path"])


@dag_with_artifact.task("script.sh", cache=True)
def create_file(path: Artifact[Path]):
    path.touch()


@dag_with_artifact.task("script.sh", cache=True)
def print_path(input_path: str):
    print(f"The input path is {input_path}")
```

As you can see, `Artifact[Path]` is just a Path. You can also use `Artifact[str]` to leave the path as a string. During the first execution with:

```sh
sdag run dag_with_artifact --local
```

the `artifact.txt` file will be created in the main project directory. During successive execution the task will be marked as cached and not scheduled again. All artifacts must exist for a task to be marked as cached. This feature has been introduced so that if you accidentally delete the artifact or move it somewhere else, the task will be executed again to produce a new one in the correct path. You can try to delete `artifact.txt` and re-execute again the pipeline to verify the cache gets invalidated. Artifact paths can be passed around between tasks via their `artifact` attribute by specifying the task name. This is very useful when one task writes some data the next one must read.

You can use the `describe` command to visualize a bunch of useful information about the tasks of a pipeline including whether they are cached, their input values, and the artifact they are expected to produce. Running:

```sh
sdag describe dag_with_artifact
```

should print something like:

```
┌─────┬─────────────┬───────────────────┬────────┬───────────┬────────────────────────────────┐
│ uid │ name        │ pipeline          │ cached │ artifacts │ kwargs                         │
├─────┼─────────────┼───────────────────┼────────┼───────────┼────────────────────────────────┤
│     │             │                   │        │           │ {                              │
│   2 │ create_file │ dag_with_artifact │ true   │ path      │   "path": "artifact.txt"       │
│     │             │                   │        │           │ }                              │
├─────┼─────────────┼───────────────────┼────────┼───────────┼────────────────────────────────┤
│     │             │                   │        │           │ {                              │
│   3 │ print_path  │ dag_with_artifact │ true   │           │   "input_path": "artifact.txt" │
│     │             │                   │        │           │ }                              │
└─────┴─────────────┴───────────────────┴────────┴───────────┴────────────────────────────────┘
```

`sdag prune` can be used to prune the cache of tasks or pipelines. Run:

```sh
sdag prune create_file -p dag_with_artifact
```

to delete the cache of the `create_file` task. Check out the [CLI](09_cli.md) section for details.
