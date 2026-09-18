"""Settings."""

import sys
from argparse import Namespace
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from pydantic import ConfigDict, Field

from sdag.models import DAG, ScriptPath, SlurmOverride, TaskNode
from sdag.types import Commands


class SDAGTag(SlurmOverride):
    """Tag configurations.

    User are allowed to override slurm variables,
    command, and script path.

    Attributes:
        tag (str): Tag name.
        cmd (Commands | None): Command. Defaults to None.
        script_path (Path | None): Script path.
    """

    tag: str
    cmd: Commands | None = None
    script_path: Path | None = None


class Pyproj(SlurmOverride):
    """pyproject.toml configs.

    Attributes:
        dag_dir (str): DAG directory.
        compiled_dag_dir (str): Compiled DAG directory.
        prepend_compiled_dag_dir (bool): Add the compiled pipeline
            dir automatically if the directory has not been
            specified.
        time_between_polls (int): Time between successive Slurm polls
            in seconds. Defaults to 5.
        commands (Commands | None): Commands to execute tasks. If
            set, it overrides all pipeline commands.
        log_level (Literal["debug", "info", "warning", "error"]):
            Logging level.
        max_concurrency (int): Maximum number of tasks executed
            at the same time.
        slurm_grace_period (int): Number of turns a task can be missing
            from the response without being considered as failed.
            Defaults to 60.
        fail_fast (bool): Kill the scheduler and all running tasks if
            any of them fails.
    """

    dag_dir: str = Field(alias="dag-dir", default="./pipelines")
    compiled_dag_dir: str = Field(
        alias="compiled-dag-dir", default="./compiled-pipelines"
    )
    cmd: Commands | None = None
    prepend_compiled_dag_dir: bool = Field(
        alias="prepend-compiled-dag-dir", default=True
    )
    log_level: Literal["debug", "info", "warning", "error"] = Field(
        alias="log-level", default="info"
    )
    time_between_polls: int = Field(
        alias="time-between-polls", default=5, ge=0
    )
    max_concurrency: int = Field(alias="max-concurrency", default=0, ge=0)
    slurm_grace_period: int = Field(
        alias="slurm-grace-period", default=60, ge=0
    )
    max_concurrent_runs: int = Field(
        alias="max-concurrent-runs", default=20, gt=0
    )
    fail_fast: bool = Field(alias="fail-fast", default=False)

    tags: list[SDAGTag] = Field(default_factory=list)

    model_config = ConfigDict(
        extra="forbid", validate_by_alias=True, serialize_by_alias=True
    )

    def join_cli_args(self, args: Namespace) -> None:
        """Join the user preferences into the pyproject.

        Args:
            args (Namespace): Parsed CLI args.
        """
        argdict = args.__dict__

        if argdict.get("log_level") is not None:
            self.log_level = args.log_level

        if argdict.get("time_between_polls") is not None:
            self.time_between_polls = args.time_between_polls

        if argdict.get("max_concurrency") is not None:
            self.max_concurrency = args.max_concurrency

        if argdict.get("compiled_dag_dir") is not None:
            self.compiled_dag_dir = args.compiled_dag_dir

        if argdict.get("fail_fast"):
            self.fail_fast = True

        if argdict.get("local"):
            self.cmd = "bash"
            for tag in self.tags:
                tag.cmd = "bash"


@lru_cache(maxsize=1)
def get_pyproj(
    pyproj_path: str = "pyproject.toml", sdag_toml_path: str = ".sdag.toml"
) -> Pyproj:
    """Get configurations.

    Args:
        pyproj_path (str, optional): pyproject.toml path. Defaults
            to "pyproject.toml".
        sdag_toml_path (str, optional): sdag.toml path. Defaults
            to ".sdag.toml".

    Returns:
        Pyproj: Parsed configurations. User settings have not been
            added.
    """
    pyproj_dict = _load_toml(pyproj_path).get("tool", {}).get("sdag", {})
    sdag_toml_dict = _load_toml(sdag_toml_path)
    joined_dict = _join_configs(pyproj_dict, sdag_toml_dict)

    return Pyproj.model_validate(joined_dict, by_alias=True)


def apply_pyproj_configs(dag: DAG, pyproj: Pyproj) -> None:
    """Apply pyproject settings to all tasks.

    Args:
        dag (DAG): Compiled DAG.
        pyproj (Pyproj): Parsed pyproject.toml.
    """
    for task in dag.nodes:
        if task.kind == "task":
            apply_pyproj_to_task(task, pyproj)


def apply_pyproj_to_task(task: TaskNode, pyproj: Pyproj) -> None:
    """Apply pyproject settings to a task.

    Args:
        task (TaskNode): Task node.
        pyproj (Pyproj): Parsed pyproject.
    """
    if pyproj.cmd is not None:
        task.cmd = pyproj.cmd
    task.slurm.override_from(pyproj)
    for tag in pyproj.tags:
        if tag.tag in task.tags:
            _apply_tag_configs(task, tag)


def _apply_tag_configs(task: TaskNode, tag: SDAGTag) -> None:
    """Override the configs from the tag ones.

    Args:
        task (TaskNode): Task.
        tag (SDAGTag): tag configurations of the task.
    """
    if tag.cmd is not None:
        task.cmd = tag.cmd
    if tag.script_path is not None:
        task.script = ScriptPath(path=tag.script_path)
    task.slurm.override_from(tag)


def _load_toml(path: str | Path) -> dict[str, Any]:
    """Load a toml file.

    Args:
        path (str | Path): Toml file path.

    Returns:
        dict[str, Any]: Loaded toml. If the file is missing,
            an empty dict is returned.
    """
    toml_path = Path(path)
    if not toml_path.is_file():
        return {}

    with toml_path.open("rb") as f:
        return tomllib.load(f)


def _join_configs(
    pyproj_dict: dict[str, Any], sdag_toml_dict: dict[str, Any]
) -> dict[str, Any]:
    """Join configurations.

    Args:
        pyproj_dict (dict[str, Any]): Loaded pyproject.toml.
        sdag_toml_dict (dict[str, Any]): Loaded sdag.toml.

    Raises:
        TypeError: Tag configs are not lists.

    Returns:
        dict[str, Any]: Joined configurations.
    """
    pyproj_tags = pyproj_dict.get("tags", [])
    sdag_toml_tags = sdag_toml_dict.get("tags", [])
    if not isinstance(pyproj_tags, list) or not isinstance(
        sdag_toml_tags, list
    ):
        msg = "Tag specification in configs must be valid lists"
        raise TypeError(msg)

    pyproj_tag_dict = _get_tag_dict(pyproj_tags)
    sdag_toml_tag_dict = _get_tag_dict(sdag_toml_tags)
    tag_dict = pyproj_tag_dict | sdag_toml_tag_dict
    configs = pyproj_dict | sdag_toml_dict
    configs["tags"] = list(tag_dict.values())

    return configs


def _get_tag_dict(tags: list) -> dict:
    """Get a dictionary mapping tag names to tags.

    Args:
        tags (list): Tag configurations.

    Raises:
        TypeError: Tag configurations are not dictionaries.
        ValueError: Tag has no name.

    Returns:
        dict: tag name -> tag config map
    """
    tag_dict: dict[Any, dict[Any, Any]] = {}
    for tag in tags:
        if not isinstance(tag, dict):
            msg = f"Tags must be valid dictionaries. Invalid tag: '{tag}'"
            raise TypeError(msg)

        tag_name = tag.get("tag")
        if not tag_name:
            msg = f"Tags must have a valid name. Invalid tag: '{tag}'"
            raise ValueError(msg)

        tag_dict[tag_name] = tag

    return tag_dict
