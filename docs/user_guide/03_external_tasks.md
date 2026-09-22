# External Tasks

By default, sdag searches pipelines in `./pipelines`. To follow along with this example, create a `./pipelines` folder containing an empty `__init__.py` and a `external_tasks.py` module. Your project tree should look like this:

```
<package-name>
└── pipelines
    ├── __init__.py        <- Do not forget to create this file!
    └── external_tasks.py  <- Module edited in this example
```

Many times, tasks are just regular Python functions wrapped by sdag. However, sometimes you want to run Bash commands, executables, or perhaps a piece of code unrelated to the `sdag` project. Tasks can be marked as external to inform sdag they do not wrap a Python function. Paste the following code into `external_tasks.py`:

```py
from sdag import Script, pipeline


@pipeline
def external_dag():
    external_task(input_data="world")


@external_dag.task(Script("echo $INPUT_DATA"), mode="ext")
def external_task(input_data: str): ...
```

You can either write a separate bash script and insert its path into the decorator or use the `Script` object to embed its content into the module. Here, the Python function is only used to define the graph node and it's not actually called by the scheduler. Uppercased input values are set as environment variables by sdag, so you can use them inside the external script. You can run this example through:

```sh
sdag run external_dag --local
```

!!! warning
    Input arguments like `path` or `user` might cause troubles as they would override the corresponding `PATH` and `USER` environment variables. For this reason, a compile-time error is thrown when the input variables of external tasks might override POSIX environment variables. The obvious solution is to use safer names like `input_path` instead of `path`.
