<div align="center">

# sdag

DAGs for Slurm.

| | |
|---|---|
| ✅ **DAG management:** Organize your workflows in clean DAGs | ✅ **Caching:** Completed jobs won't run again |
| ✅ **Simple API:** Familiar Python API equipped with a Rust scheduler | ✅ **Failure recovery:** Handle errors and retries |
| ✅ **Visualization:** View workflows and details directly in the terminal | ✅ **Control flow:** Adapt your pipelines dynamically |
| ✅ **Artifacts:** Verify the expected products of a task | ✅ **Local or Slurm:** Run on your laptop or scale across a Slurm cluster |

</div>

## Getting started

After installing the library, copy the following snippet into a file named `pipeline.py`:

```py
from sdag import Script, pipeline


@pipeline
def hello() -> None:
    say_hello(name="sdag")


@hello.task(Script("sdag-execute"), cmd="bash")
def say_hello(name: str) -> None:
    print(f"hello from {name}")
```

To run the `hello` pipeline, execute:

```sh
sdag run pipeline:hello
```

Check out the [sdag examples](https://github.com/igeniusai/sdag_examples) for end-to-end examples.
