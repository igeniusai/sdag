# sdag

DAGs for Slurm.

## Getting started

After cloning and installing this repo as editable:

```sh
uv add --editable sdag
```

the following example shows how to create and compile a pipeline:

```py
from sdag import sdag


@sdag.task(launch_script="submit.sh")
def hello_world():
    print("Hello, world!")


@sdag.pipeline
def pipeline():
    hello_world()

sdag.compile(pipeline)
```

Check out the [sdag examples](https://github.com/igeniusai/sdag_examples) for end-to-end examples.
