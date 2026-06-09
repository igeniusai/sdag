# sdag

DAGs for Slurm.

## Getting started

After installing the library, copy the following snippet into a file named `pipeline.py`:

```py
from sdag import Script, pipeline, task


@pipeline
def hello() -> None:
    say_hello(name="sdag")


@task(Script("sdag-execute"), cmd="bash")
def say_hello(name: str) -> None:
    print(f"hello from {name}")
```

To run the `hello` pipeline, execute:

```sh
sdag run pipeline:hello
```

Check out the [sdag examples](https://github.com/igeniusai/sdag_examples) for end-to-end examples.
