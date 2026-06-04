from pathlib import Path

from sdag4 import Artifact, pipeline, task


@pipeline
def foopipe():
    task = hello3()
    t4 = hello4(input_path=task.artifacts["input_path1"])
    hello5(t4, input_path=".sdag")


@task("butta.sh")
def hello3(
    input_path1: Artifact[str] = "a/path2",
    input_path2: Artifact[Path] = "a/path2",  # type: ignore
):
    print("3", input_path1, type(input_path1))
    print("3", input_path2, type(input_path2))


@task("butta.sh", cache=True)
def hello4(input_path: Path):
    print("4", input_path, type(input_path))


@task("butta.sh", cache=True)
def hello5(input_path: Artifact[Path]):
    print("5", input_path, type(input_path))


@pipeline
def breaking():
    task1 = will_break()
    will_break(task1)


@task("butta.sh", debug=True)
def will_break():
    """Will fail."""
    raise ValueError
