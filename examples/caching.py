from pathlib import Path

from sdag import Artifact, pipeline


@pipeline
def cached():
    """DAG with cached and non-cached tasks."""
    t = cached_task(a="world")
    non_cached_task(t)


@cached.task("scripts/submit.sh", cache=True)
def cached_task(a: str):
    """Cacheable task.

    Args:
        a (str): Cached input value.
    """
    print(f"I only run once. The value of a is {a}")


@cached.task("scripts/submit.sh")
def non_cached_task():
    """Task without caching."""
    print("I run every time")


@pipeline
def name_change():
    """Cached tasks with different names."""
    a_task(value=1)
    t = a_task(value=2)
    t.name = "another_task"


@name_change.task("scripts/submit.sh", cache=True)
def a_task(value: int):
    """Cacheable task."""
    print(f"value: {value}")


@pipeline
def dag_with_artifact():
    """DAG with artifact."""
    t = create_file(path="artifact.txt")
    print_path(input_path=t.artifacts["path"])


@dag_with_artifact.task("scripts/submit.sh", cache=True)
def create_file(path: Artifact[Path]):
    """Create an artifact."""
    path.touch()


@dag_with_artifact.task("scripts/submit.sh", cache=True)
def print_path(input_path: str):
    """Print the input path."""
    print(f"The input path is {input_path}")
