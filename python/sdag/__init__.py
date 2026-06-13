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

from sdag.entrypoints import sdag_execute
from sdag.models import Artifact
from sdag.visualization import DAGViewer
from sdag.wrappers import Elif, Else, If, Script, oneof, pipeline, task

__all__ = [
    "Artifact",
    "DAGViewer",
    "Elif",
    "Else",
    "If",
    "Script",
    "oneof",
    "pipeline",
    "sdag_execute",
    "task",
]
