"""End-to-end tests."""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from sdag.__main__ import main
from sdag.commands import _parse_json_input


@pytest.fixture
def set_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[None]:
    """Safely set the home directory.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.

    Yields:
        Generator[None]: Drops `SDAG_HOME` at the end of the test.
    """
    monkeypatch.setenv("SDAG_HOME", str(tmp_path))
    yield
    monkeypatch.delenv("SDAG_HOME")


@pytest.mark.parametrize(
    argnames=("data", "res"), argvalues=[(None, None), ('{"a": 1}', {"a": 1})]
)
def test_parse_json_input(data: str | None, res: Any) -> None:
    """Test the JSON string parsing.

    Args:
        data (str | None): JSON string input data.
        res (Any): Expected result.
    """
    parsed = _parse_json_input(data)
    assert parsed == res


def test_fail_parse_json_input() -> None:
    """Input string is incorrect."""
    data = '{"a"'
    with pytest.raises(json.decoder.JSONDecodeError):
        _parse_json_input(data)


@pytest.mark.usefixtures("set_home")
def test_prune_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test the cache pruning of a task named 'all'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    cache_path = tmp_path / ".sdag" / ".cache"
    cached_task_path = cache_path / "all"
    cached_task_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("sys.argv", ["sdag", "prune", "all"])

    main()

    assert not cached_task_path.exists()
    assert cache_path.exists()


@pytest.mark.usefixtures("set_home")
def test_kill_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the job killing.

    The pipeline has no running jobs, so nothing happens.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    pipeline_name = "pipeline"
    pipeline = {
        "meta": {
            "name": pipeline_name,
            "creation_dt": "2025-12-30T11:30:46.343072",
        },
        "nodes": [
            {
                "uid": "0",
                "output_artifacts": [],
                "behavior": {
                    "type": "TaskNode",
                    "fname": "task",
                    "name": "task",
                    "caching": True,
                    "mode": "wrap",
                    "cmd": "sbatch",
                    "try_num": 1,
                    "retries": 0,
                    "launch_script": "submit.sh",
                    "input_kwargs": [],
                },
                "status": "NotSubmitted",
                "parents": [],
                "children": [],
            }
        ],
    }

    pipeline_path = tmp_path / ".sdag" / pipeline_name
    # 0 is needed because checkpoints are validated
    (pipeline_path / "0").mkdir(parents=True, exist_ok=True)
    with Path(pipeline_path / "checkpoint.json").open("w") as f:
        json.dump(pipeline, f)

    monkeypatch.setattr("sys.argv", ["sdag", "kill", pipeline_name])
    main()


@pytest.mark.usefixtures("set_home")
def test_run_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test the pipeline execution.

    The only stage is local and external, it creates a
    file named 'target.txt'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    target_path = tmp_path / "target.txt"
    script_path = tmp_path / "submit.sh"
    pipeline_path = tmp_path / "pipeline.json"

    with script_path.open("w") as f:
        f.write(f"touch {target_path}")

    pipeline = {
        "meta": {
            "name": "pipeline",
            "creation_dt": "2025-12-30T11:30:46.343072",
        },
        "nodes": [
            {
                "uid": "_pipeline_0_root_",
                "output_used": False,
                "parents": [],
                "behavior": {"type": "RootNode"},
                "output_artifacts": [],
            },
            {
                "uid": "0",
                "output_artifacts": [],
                "behavior": {
                    "type": "TaskNode",
                    "fname": "task",
                    "name": "task",
                    "caching": False,
                    "mode": "ext",
                    "cmd": "bash",
                    "try_num": 1,
                    "retries": 0,
                    "launch_script": str(script_path),
                    "input_kwargs": [{"key": "k", "value": "v"}],
                },
                "parents": [
                    {
                        "parent_type": {"type": "Logical"},
                        "uid": "_pipeline_0_root_",
                    }
                ],
            },
        ],
    }

    with pipeline_path.open("w") as f:
        json.dump(pipeline, f)

    monkeypatch.setattr(
        "sys.argv",
        ["sdag", "run", f"{pipeline_path}", "-w", "0"],
    )
    main()

    assert (tmp_path / ".sdag" / "pipeline" / "0" / "output.json").exists()
    assert target_path.exists()


@pytest.mark.usefixtures("set_home")
def test_run_pipeline_with_caching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test pipeline execution and task caching.

    This also verifies the task name is used instead
    of fname.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    script_path = tmp_path / "submit.sh"
    pipeline_path = tmp_path / "pipeline.json"

    with script_path.open("w") as f:
        f.write("echo hello")

    pipeline = {
        "meta": {
            "name": "pipeline",
            "creation_dt": "2025-12-30T11:30:46.343072",
        },
        "nodes": [
            {
                "uid": "_pipeline_0_root_",
                "output_used": False,
                "parents": [],
                "behavior": {"type": "RootNode"},
                "output_artifacts": [],
            },
            {
                "uid": "0",
                "output_artifacts": [],
                "behavior": {
                    "type": "TaskNode",
                    "fname": "task",
                    "name": "task_name",
                    "caching": True,
                    "mode": "ext",
                    "cmd": "bash",
                    "try_num": 1,
                    "retries": 0,
                    "launch_script": str(script_path),
                    "input_kwargs": [{"key": "k", "value": "v"}],
                },
                "parents": [
                    {
                        "parent_type": {"type": "Logical"},
                        "uid": "_pipeline_0_root_",
                    }
                ],
            },
        ],
    }

    with pipeline_path.open("w") as f:
        json.dump(pipeline, f)

    monkeypatch.setattr(
        "sys.argv",
        ["sdag", "run", f"{pipeline_path}", "-w", "0"],
    )
    main()

    cache_path = tmp_path / ".sdag" / ".cache" / "task_name"
    assert (cache_path / "output.json").exists()
    assert (cache_path / "input.json").exists()
    assert (cache_path / "meta.json").exists()
