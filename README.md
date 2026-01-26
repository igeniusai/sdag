# sdag

DAGs for Slurm.

## Getting started

After installing the library, copy the following snippet into a file named `pipeline.py`:

```py
from sdag import sdag


@sdag.task(launch_script="submit.sh", mode="ext", cmd="bash")
def hello_world(name: str): ...


@sdag.pipeline
def pipeline():
    hello_world(name="sdag")
```

and create a new scipt named `script.sh` with the following content:

```sh
echo Hello from $NAME
```

To compile and run the pipeline locally, execute:

```sh
sdag cr pipeline:pipeline --local
```

Check out the [sdag examples](https://github.com/igeniusai/sdag_examples) for end-to-end examples.
