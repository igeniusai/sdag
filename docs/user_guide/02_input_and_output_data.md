# Input and Output Data

Keyword arguments can be used to take as input the output of a previous task.
Create the following two files in the main project directory:

=== "`input_and_output.py`"

    ```py
    from sdag import pipeline


    @pipeline
    def output_use():
        t = return_output()
        take_input(world=t)


    @output_use.task("script.sh")
    def return_output():
        return "world"


    @output_use.task("script.sh")
    def take_input(world: str):
        print(f"Hello, {world}!")
    ```

=== "`script.sh`"

    ```sh
    sdag-execute
    ```


`sdag view output_use` will print:

```
   ╭──────╮
   │_root_│
   ╰──────╯
       │
       │
       │
       ▼
┌─────────────┐
│return_output│
└─────────────┘
       │
       │
       │output
       ▼
 ┌──────────┐
 │take_input│
 └──────────┘
```

So, the output of `return_output` is used as input in the `take_input` task.

!!! warning
    Output data is serialized as JSON under the hood, which means only JSON-serializable objects are allowed (e.g., lists are fine but tuples are not). Because we don't want to enforce default serialization methods, when working with complex or large data structures it is recommended to serialize them separately and move their paths around as [artifacts](06_caching_artifacts.md).

You can run the example via:

```sh
sdag run output_use --local
```

Pipelines can take input arguments as well. Add the following code to the same module:

```py
from pathlib import Path


@pipeline
def dag_with_input(input_path):
    use_input_path(input_path=input_path)


@dag_with_input.task("script.sh")
def use_input_path(input_path: Path):
    print(f"Input path: '{input_path}'; type: {type(input_path)}")
```

Input argument can be passed to the pipeline as extra CLI arguments:

```sh
sdag run dag_with_input --local --input-path=a/path
```

You can use `--` to make sure extra arguments are not overshadowed by the sdag CLI:

```sh
sdag run dag_with_input --local -- --input-path=a/path
```

It is also possible to run a specific task of a pipeline via `sdag runtask`. One must specify task, pipeline it belongs to, and pass all input arguments as extra CLI args:

```sh
sdag runtask use_input_path dag_with_input --local --input-path=a/path
```

Notice that `input_path` is automatically cast to `Path`. We only do this with paths to eliminate a frequent pattern showing up in our pipelines:

```py
@dag_with_input.task("script.sh")
def use_input_path(input1: str, input2: str):
    input1_path = Path(input1)
    input2_path = Path(input2)
    ...
```

!!! info
    To run these examples with Slurm, turn `script.sh` into a [sbatch](https://slurm.schedmd.com/sbatch.html) script as explained in the [Hello World](00_hello_world.md) section and execute pipelines without the `--local` option.
