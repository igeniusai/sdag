"""sdag.

Copy the following snippet into a hello.py file.

```py
from sdag import pipeline, task, Script


@pipeline
def dag():
    say_hello(world="world")

@task(Script("echo hello $world"), mod="ext", cmd="bash")
def say_hello(world: str) -> None: ...
```

Run `sdag run hello:dag` to execute the pipeline.
"""

from sdag4.entrypoints import sdag_execute
from sdag4.models import Artifact
from sdag4.visualization import DAGViewer
from sdag4.wrappers import Elif, Else, If, Script, pipeline, task

__all__ = [
    "Artifact",
    "DAGViewer",
    "Elif",
    "Else",
    "If",
    "Script",
    "pipeline",
    "sdag_execute",
    "task",
]
