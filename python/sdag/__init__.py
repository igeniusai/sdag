# SPDX-FileCopyrightText: 2026 Domyn
# SPDX-License-Identifier: Apache-2.0

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

from sdag._version import __version__
from sdag.discovery import (
    find_all_pipelines,
    find_pipeline_by_name,
    get_importable_module_path,
)
from sdag.entrypoints import sdag_execute
from sdag.models import Artifact, ScriptContent, ScriptPath
from sdag.visualization import DAGViewer
from sdag.wrappers import (
    Elif,
    Else,
    If,
    Script,
    oneof,
    pipeline,
    task,
)

__all__ = [
    "Artifact",
    "DAGViewer",
    "Elif",
    "Else",
    "If",
    "Script",
    "ScriptContent",
    "ScriptPath",
    "__version__",
    "find_all_pipelines",
    "find_pipeline_by_name",
    "get_importable_module_path",
    "oneof",
    "pipeline",
    "sdag_execute",
    "task",
]
