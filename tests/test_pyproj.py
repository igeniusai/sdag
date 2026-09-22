# SPDX-FileCopyrightText: 2026 Domyn
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Any

import pytest
from sdag.models import DAG, DAGMeta, ScriptPath, TaskNode
from sdag.pyproj import Pyproj, apply_pyproj_configs, get_pyproj


def test_parse_pyproject(tmp_path: Path) -> None:
    """Check pyproject is parsed correctly.

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    content = """[tool.sdag]
    dag-dir = "path/to/dagdir"
    compiled-dag-dir = "path/to/compiled"
    prepend-compiled-dag-dir = true
    log-level = "debug"
    slurm-grace-period = 50
    max-concurrent-runs = 2
    cmd = "bash"

    [[tool.sdag.tags]]
    tag = "online"
    cmd = "sbatch"
    """
    pyproj_path = tmp_path / "pyproject.toml"
    # so we don't read the main one by accident
    sdag_toml_path = tmp_path / "missing.toml"
    with pyproj_path.open("w") as f:
        f.write(content)

    pyproj = get_pyproj(
        pyproj_path=str(pyproj_path), sdag_toml_path=str(sdag_toml_path)
    )
    assert pyproj.dag_dir == "path/to/dagdir"
    assert pyproj.compiled_dag_dir == "path/to/compiled"
    assert pyproj.prepend_compiled_dag_dir
    assert pyproj.cmd == "bash"
    assert pyproj.log_level == "debug"
    assert pyproj.slurm_grace_period == 50
    assert pyproj.max_concurrent_runs == 2
    assert pyproj.tags[0].tag == "online"
    assert pyproj.tags[0].cmd == "sbatch"


def test_parse_pyproject_invalid_tags(tmp_path: Path) -> None:
    """Tags must be formatted as lists with [[..]]

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    content = """[tool.sdag.tags]
    tag = "online"
    cmd = "sbatch"
    """
    path = tmp_path / "pyproject.toml"
    with path.open("w") as f:
        f.write(content)

    with pytest.raises(TypeError, match="list"):
        get_pyproj(str(path))


def test_parse_pyproject_no_tag_name(tmp_path: Path) -> None:
    """Tags must always contain the tag name.

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    content = """[[tool.sdag.tags]]
    cmd = "sbatch"
    """
    path = tmp_path / "pyproject.toml"
    with path.open("w") as f:
        f.write(content)

    with pytest.raises(ValueError, match="name"):
        get_pyproj(str(path))


def test_parse_pyproject_tag_is_not_obj(tmp_path: Path) -> None:
    """Tags must be objects.

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    content = r"tool = {sdag = {tags = [2, 3, 4]}}"
    path = tmp_path / "pyproject.toml"
    with path.open("w") as f:
        f.write(content)

    with pytest.raises(TypeError, match="dictionaries"):
        get_pyproj(str(path))


def test_parse_sdag_toml(tmp_path: Path) -> None:
    """Check the sdag.toml is parsed correctly.

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    content = """dag-dir = "path/to/dagdir"
    compiled-dag-dir = "path/to/compiled"
    prepend-compiled-dag-dir = true
    log-level = "debug"
    slurm-grace-period = 50
    max-concurrent-runs = 2
    cmd = "bash"

    [[tags]]
    tag = "online"
    cmd = "sbatch"
    """
    sdag_toml_path = tmp_path / ".sdag.toml"
    with sdag_toml_path.open("w") as f:
        f.write(content)

    missing_pyproj_path = tmp_path / "missing.toml"
    pyproj = get_pyproj(
        pyproj_path=str(missing_pyproj_path),
        sdag_toml_path=str(sdag_toml_path),
    )

    assert pyproj.dag_dir == "path/to/dagdir"
    assert pyproj.compiled_dag_dir == "path/to/compiled"
    assert pyproj.prepend_compiled_dag_dir
    assert pyproj.cmd == "bash"
    assert pyproj.log_level == "debug"
    assert pyproj.slurm_grace_period == 50
    assert pyproj.max_concurrent_runs == 2
    assert pyproj.tags[0].tag == "online"
    assert pyproj.tags[0].cmd == "sbatch"


def test_join_pyproj_and_sdag_toml(tmp_path: Path) -> None:
    """Check pyproj and sdag.toml are merged correctly.

    Args:
        tmp_path (Path): Temporary path fixture.
    """
    pyproj_content = """[tool.sdag]
    dag-dir = "path/to/dagdir"
    compiled-dag-dir = "path/to/compiled"
    prepend-compiled-dag-dir = true
    slurm-grace-period = 20
    cmd = "bash"

    [[tool.sdag.tags]]
    tag = "online"
    cmd = "sbatch"

    [[tool.sdag.tags]]
    tag = "large"
    cmd = "bash"
    """

    sdag_toml_content = """compiled-dag-dir = "path-alt"
    prepend-compiled-dag-dir = false
    log-level = "debug"
    cmd = "bash"

    [[tags]]
    tag = "online"
    cmd = "bash"

    [[tags]]
    tag = "small"
    cmd = "bash"
    """

    pyproj_path = tmp_path / "pyproj.toml"
    with pyproj_path.open("w") as f:
        f.write(pyproj_content)

    sdag_toml_path = tmp_path / "sdag.toml"
    with sdag_toml_path.open("w") as f:
        f.write(sdag_toml_content)

    pyproj = get_pyproj(
        pyproj_path=str(pyproj_path), sdag_toml_path=str(sdag_toml_path)
    )
    pyproj.tags.sort(key=lambda t: t.tag)

    assert pyproj.dag_dir == "path/to/dagdir"
    assert pyproj.compiled_dag_dir == "path-alt"
    assert not pyproj.prepend_compiled_dag_dir
    assert pyproj.cmd == "bash"
    assert pyproj.log_level == "debug"
    assert pyproj.slurm_grace_period == 20

    assert pyproj.tags[0].tag == "large"
    assert pyproj.tags[0].cmd == "bash"
    assert pyproj.tags[1].tag == "online"
    assert pyproj.tags[1].cmd == "bash"
    assert pyproj.tags[2].tag == "small"
    assert pyproj.tags[2].cmd == "bash"


@pytest.mark.parametrize(
    argnames=("pyproj_dict", "cmd", "script_path"),
    argvalues=[
        # task + null -> task
        ({}, "sbatch", ""),
        # task + global -> global
        ({"cmd": "bash"}, "bash", ""),
        # task + global + null -> global
        ({"cmd": "bash", "tags": [{"tag": "TAG1"}]}, "bash", ""),
        # task + global + TAG1 -> TAG1
        (
            {"cmd": "bash", "tags": [{"tag": "TAG1", "cmd": "sbatch"}]},
            "sbatch",
            "",
        ),
        # task + global + TAG1 + TAG2 -> TAG2
        (
            {
                "cmd": "bash",
                "tags": [
                    {"tag": "TAG1", "cmd": "sbatch"},
                    {"tag": "TAG2", "cmd": "bash"},
                ],
            },
            "bash",
            "",
        ),
        # Replace script tag
        ({"tags": [{"tag": "TAG1", "script_path": "path"}]}, "sbatch", "path"),
    ],
)
def test_apply_pyproj_configs(
    pyproj_dict: dict[str, Any], cmd: str, script_path: str
) -> None:
    """Check override priorities are respected.

    Args:
        pyproj_dict (dict[str, Any]): Pyproj dictionary.
        cmd (str): Task command.
        script_path (str): Submission script path.
    """
    pyproj = Pyproj.model_validate(pyproj_dict)
    task = TaskNode(
        uid=0,
        name="task_name",
        fn_name="task_fn",
        cache=False,
        mode="ext",
        cmd="sbatch",
        scope="local",
        retries=0,
        tags=["TAG1", "TAG2"],
        script=ScriptPath(path=Path()),
    )

    dag = DAG(
        meta=DAGMeta(pipeline_name="pipe", hash="xxx"),
        nodes=[task],
    )

    apply_pyproj_configs(dag, pyproj)
    assert dag.nodes[0].cmd == cmd  # type: ignore
    assert dag.nodes[0].script == ScriptPath(path=Path(script_path))  # type: ignore


def test_override_all_slurm_cmds() -> None:
    """Check slurm commands are overwritten."""
    task = TaskNode(
        uid=0,
        name="task_name",
        fn_name="task_fn",
        cache=False,
        mode="ext",
        cmd="sbatch",
        scope="local",
        retries=0,
        tags=[],
        script=ScriptPath(path=Path()),
    )
    dag = DAG(
        meta=DAGMeta(pipeline_name="pipe", hash="xxx"),
        nodes=[task],
    )
    pyproj_dict = {
        "job-name": "job",
        "nodes": 2,
        "partition": "partiton",
        "qos": "qos",
        "gpus-per-node": 4,
        "ntasks-per-node": 3,
        "output": "out",
        "error": "err",
        "account": "acc",
        "cpus-per-task": 1,
        "mem": "1GB",
        "time": "00:01:00",
    }

    pyproj = Pyproj.model_validate(pyproj_dict)
    apply_pyproj_configs(dag, pyproj)

    assert task.slurm.model_dump() == pyproj_dict


def test_override_slurm_from_tag() -> None:
    """Check slurm commands are overwritten by tags."""
    task = TaskNode(
        uid=0,
        name="task_name",
        fn_name="task_fn",
        cache=False,
        mode="ext",
        cmd="sbatch",
        scope="local",
        retries=0,
        tags=["TAG2"],
        script=ScriptPath(path=Path()),
    )
    dag = DAG(
        meta=DAGMeta(pipeline_name="pipe", hash="xxx"),
        nodes=[task],
    )
    pyproj_dict = {
        "job-name": "job-1",
        "nodes": 1,
        "partition": "partiton-1",
        "qos": "qos-1",
        "gpus-per-node": 1,
        "ntasks-per-node": 1,
        # output intentionally skipped
        "error": "err-1",
        "account": "acc-1",
        "cpus-per-task": 1,
        "mem": "1GB",
        "time": "00:01:00",
        "tags": [
            {
                "tag": "TAG2",
                "job-name": "job-2",
                "nodes": 2,
                "partition": "partiton-2",
                "qos": "qos-2",
                "gpus-per-node": 2,
                "ntasks-per-node": 2,
                "output": "out-2",
                "error": "err-2",
                "account": "acc-2",
                "cpus-per-task": 2,
                "mem": "2GB",
                "time": "00:02:00",
            }
        ],
    }

    pyproj = Pyproj.model_validate(pyproj_dict)
    apply_pyproj_configs(dag, pyproj)

    tag_slurm = pyproj_dict["tags"][0].copy()
    tag_slurm.pop("tag")

    assert task.slurm.model_dump() == tag_slurm
