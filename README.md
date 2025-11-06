# sdag

DAGs for Slurm.

## Getting started

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
