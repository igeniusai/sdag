# Tags

Tags can be used to specify custom configurations for groups of tasks. Tags can be added to tasks via the `tags` argument of the `@task`or `@<pipeline-name>.task` decorator:

```py
from sdag import task

@task("script.sh", tags=["online", "small", "custom"])
def some_task(): ...
```

The corresponding configurations be either set in the `pyproject.toml` under `[[tools.sdag.tags]]` or in the `.sdag.toml` file under `[[tags]]`.
Here is how to define three tags in the `pyproject.toml`:

```toml
[[tool.sdag.tags]]
tag = "online"
cmd = "bash"

[[tool.sdag.tags]]
tag = "small"
cpus-per-task = 1

[[tool.sdag.tags]]
tag = "custom"
script-path = "a/custom/script.sh"
```

Tags have higher priority than pipeline code and global `pyproject.toml`/`.sdag.toml` configurations. When a task is assigned more than one tag, the configurations are applied in the same order they are provided. Tags can be used to customize the behavior of groups of tasks. For examples, many public supercomputers have dedicated queues for jobs requiring internet access. Some other only allow output connection on login nodes, so tasks must run locally. Tags can be used to enforce such policies. Moreover, Tags specified in the `.sdag.toml` file override those in the `pyproject.toml`. So, different `.sdag.toml` files can be used to make tasks run transparently across different supercomputers having different policies without changing the code. Tags currently allow one to customize `cmd`, execution script path via the `script-path` key, and all Slurm settings available in the `pyproject.toml`.