# Overriding Configurations at Compile Time

When called inside a pipeline, tasks return their corresponding nodes in the graph. These nodes expose most configurations as attributes, so they can be modified at compile time to override their default values. Here is an example:


```py
from sdag import ScriptPath, pipeline


@pipeline
def override():
    t = default_task()
    t.name = "custom_name"
    t.cache = True
    t.cache_size = 5
    t.script = ScriptPath(path="another_script.sh")
    t.cmd = "bash"

    t.envs["HELLO"] = "world"

    t.slurm.job_name = "default_job"
    t.slurm.account = "my-account"
    t.slurm.time = "00:00:30"
    t.slurm.output = "path/to/file.out"


@override.task("script.sh")
def default_task(): ...
```

As you can see, it is possible to:

- Modify properties set in the decorator like the name, the script, and the caching behavior.
- Define environment variables that will be automatically set by the scheduler so you can access them during the task execution.
- Override Slurm configurations set in the execution script. These are ignored if the task runs as a local bash process.

When compared with the `pyproject.toml` configurations, the ones set inside pipelines have lower priority and are thus overwritten if conflicting.