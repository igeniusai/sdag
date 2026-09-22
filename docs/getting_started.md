# Getting Started

Copy the following snippet into a `hello.py` file:

```py
from sdag import Script, pipeline


@pipeline
def hello_world():
    say_hello(name="sdag")


@hello_world.task(Script("sdag-execute"), cmd="bash")
def say_hello(name: str) -> None:
    print(f"Hello from {name}!")
```

To compile and run the pipeline as a standalone script, execute:

```sh
sdag run hello:hello_world
```
