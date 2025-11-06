# sdag

DAGs for Slurm.

## Getting started

sdag creates a working directory to store stage outputs (`~/.sdag` by default). You can export the `SDAG_HOME` environment variable to point to a different path. It's a good idea to set the variable in the `.bashrc` file.

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

Check out the `sdag-examples` repository for more complex examples.
