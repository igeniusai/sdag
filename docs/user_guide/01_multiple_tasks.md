# Handling Multiple Tasks

By default, sdag searches pipelines in `./pipelines`. To follow along with this example, create a `./pipelines` folder containing an empty `__init__.py` and a `many_tasks.py` module. Your project tree should look like this:

```
<package-name>
└── pipelines
    ├── __init__.py   <- Do not forget to create this file!
    └── many_tasks.py <- Module edited in this example
```

Add the following code to `many_tasks.py`

```py
from sdag import pipeline


@pipeline
def many_tasks():
    say_hello()
    say_hello()
    say_hello()


@many_tasks.task("script.sh")
def say_hello():
    print("hello world!")
```

You can view the graph of this pipeline in the terminal via `sdag view`:

```sh
sdag view many_tasks
```

which should print something like:

```
                    ╭──────╮
                    │_root_│
                    ╰──────╯
                        │
                        │
     ┌──────────────────└──────────────────┐
     ▼                  ▼                  ▼
┌─────────┐        ┌─────────┐        ┌─────────┐
│say_hello│        │say_hello│        │say_hello│
└─────────┘        └─────────┘        └─────────┘
```

As these tasks do not depend on each other, they will be scheduled at the same time. You can confirm this by running the pipeline:

```sh
sdag run many_tasks --local
```

You can use positional arguments to signal graph edges. Let's add a second pipeline to the same `many_tasks.py` module:

```py
import asyncio

@pipeline
def dependent_tasks():
    t1 = first()
    t2 = first()
    second(t1, t2)


@dependent_tasks.task("script.sh")
def first():
    print("I run first!")


@dependent_tasks.task("script.sh")
async def second():
    await asyncio.sleep(1)
    print("I run second!")
```

Notice async tasks are supported: in such cases sdag will start the event loop for you. `sdag view dependent_tasks` will now print:

```
       ╭──────╮
       │_root_│
       ╰──────╯
           │
           │
   ┌───────└──────┐
   ▼              ▼
┌─────┐        ┌─────┐
│first│        │first│
└─────┘        └─────┘
   │              │
   │              │
   └───────┌──────┘
           ▼
       ┌──────┐
       │second│
       └──────┘
```

Which means that `second` will only run if both `first` tasks successfully complete. This example can be run via:

```sh
sdag run dependent_tasks --local
```
