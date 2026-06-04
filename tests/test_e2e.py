"""End-to-end tests."""

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from sdag4.entrypoints import cli


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


@pytest.mark.usefixtures("set_home")
def test_prune_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test the cache pruning of a task named 'all'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    cache_path = tmp_path / ".sdag" / ".cache"
    cached_task_path = cache_path / "global" / "all"
    cached_task_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("sys.argv", ["sdag", "prune", "all"])

    cli()

    assert not cached_task_path.exists()
    assert cache_path.exists()


@pytest.mark.usefixtures("set_home")
def test_prune_local_task(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test the cache pruning of a task named 'all'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    cache_path = tmp_path / ".sdag" / ".cache"
    cached_task_path = cache_path / "local" / "dag" / "task_name"
    cached_task_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "sys.argv", ["sdag", "prune", "task_name", "-p", "dag"]
    )

    cli()

    assert not cached_task_path.exists()
    assert cache_path.exists()


@pytest.mark.usefixtures("set_home")
def test_prune_entire_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test the cache pruning of a task named 'all'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    cache_path = tmp_path / ".sdag" / ".cache"
    cache_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("sys.argv", ["sdag", "prune", "all"])

    cli()

    assert not cache_path.exists()


@pytest.mark.usefixtures("set_home")
def test_prune_cache_from_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Test the cache pruning of a task named 'all'.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """

    pipeline_path = tmp_path / "pipeline.json"

    pipeline = {
        "meta": {
            "pipeline_name": "dag",
            "timestamp": "2025-12-30T11:30:46.343072",
            "hash": "xyzk",
            "extra": {},
            "import_path": "path.to.pipeline:fn",
        },
        "nodes": [
            {"uid": 0, "pipeline_name": "dag", "kind": "root"},
            {
                "uid": 1,
                "kind": "task",
                "fn_name": "task",
                "name": "task_global",
                "pipeline_name": "dag",
                "cache": True,
                "cache_local": False,
                "debug": False,
                "mode": "ext",
                "cmd": "bash",
                "try_num": 1,
                "retries": 0,
                "script": {"kind": "script_path", "path": "script.sh"},
                "kwargs": [{"key": "k", "value": "v"}],
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 0,
                    }
                ],
                "artifacts": [],
            },
            {
                "uid": 2,
                "kind": "task",
                "fn_name": "task",
                "name": "task_local",
                "pipeline_name": "dag",
                "cache": False,
                "cache_local": True,
                "debug": False,
                "mode": "ext",
                "cmd": "bash",
                "try_num": 1,
                "retries": 0,
                "script": {"kind": "script_path", "path": "script.sh"},
                "kwargs": [{"key": "k", "value": "v"}],
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 0,
                    }
                ],
                "artifacts": [],
            },
            {
                "uid": 2,
                "pipeline_name": "dag",
                "kind": "end",
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 1,
                    },
                    {
                        "kind": {"kind": "logical"},
                        "uid": 2,
                    },
                ],
            },
        ],
    }

    with pipeline_path.open("w") as f:
        json.dump(pipeline, f)

    cache_path = tmp_path / ".sdag" / ".cache"
    local_cache_path = cache_path / "local" / "dag" / "task_local"
    global_cache_path = cache_path / "global" / "task_global"
    global_cache_path.mkdir(parents=True, exist_ok=True)
    local_cache_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "sys.argv", ["sdag", "prune", "-p", str(pipeline_path)]
    )

    cli()

    assert cache_path.exists()
    assert not local_cache_path.exists()
    assert not global_cache_path.exists()


@pytest.mark.usefixtures("set_home")
def test_kill_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the job killing.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    pipeline_name = "pipeline"
    pipeline_hash = "xyz"
    pipeline_path = (
        tmp_path / ".sdag" / "pipelines" / pipeline_name / pipeline_hash
    )
    pipeline_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "sys.argv", ["sdag", "kill", pipeline_name, "--hash", pipeline_hash]
    )
    cli()
    assert pipeline_path.joinpath("kill.lock").is_file()


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
            "pipeline_name": "dag",
            "timestamp": "2025-12-30T11:30:46.343072",
            "hash": "xyz",
            "extra": {},
            "import_path": "path.to.pipeline:fn",
        },
        "nodes": [
            {"uid": 0, "pipeline_name": "dag", "kind": "root"},
            {
                "uid": 1,
                "kind": "task",
                "fn_name": "task",
                "name": "task",
                "pipeline_name": "dag",
                "cache": False,
                "cache_local": False,
                "debug": False,
                "mode": "ext",
                "cmd": "bash",
                "try_num": 1,
                "retries": 0,
                "script": {"kind": "script_path", "path": str(script_path)},
                "kwargs": [{"key": "k", "value": "v"}],
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 0,
                    }
                ],
                "artifacts": [],
            },
            {
                "uid": 2,
                "pipeline_name": "dag",
                "kind": "end",
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 1,
                    }
                ],
            },
        ],
    }

    with pipeline_path.open("w") as f:
        json.dump(pipeline, f)

    monkeypatch.setattr(
        "sys.argv",
        ["sdag", "run", f"{pipeline_path}", "-t", "0"],
    )
    cli()

    assert (
        tmp_path / ".sdag" / "pipelines" / "dag" / "xyz" / "1" / "output.json"
    ).exists()
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
            "pipeline_name": "dag",
            "timestamp": "2025-12-30T11:30:46.343072",
            "hash": "xyzk",
            "extra": {},
            "import_path": "path.to.pipeline:fn",
        },
        "nodes": [
            {"uid": 0, "pipeline_name": "dag", "kind": "root"},
            {
                "uid": 1,
                "kind": "task",
                "fn_name": "task",
                "name": "task",
                "pipeline_name": "dag",
                "cache": True,
                "cache_local": True,
                "debug": False,
                "mode": "ext",
                "cmd": "bash",
                "try_num": 1,
                "retries": 0,
                "script": {"kind": "script_path", "path": str(script_path)},
                "kwargs": [{"key": "k", "value": "v"}],
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 0,
                    }
                ],
                "artifacts": [],
            },
            {
                "uid": 2,
                "pipeline_name": "dag",
                "kind": "end",
                "parents": [
                    {
                        "kind": {"kind": "logical"},
                        "uid": 1,
                    }
                ],
            },
        ],
    }

    with pipeline_path.open("w") as f:
        json.dump(pipeline, f)

    monkeypatch.setattr(
        "sys.argv",
        ["sdag", "run", f"{pipeline_path}", "-t", "0"],
    )
    cli()

    cache_path = tmp_path / ".sdag" / ".cache" / "global" / "task"
    assert (cache_path / "output.json").exists()
    assert (cache_path / "input.json").exists()
    assert (cache_path / "meta.json").exists()

    cache_local_path = tmp_path / ".sdag" / ".cache" / "local" / "dag" / "task"
    assert (cache_local_path / "output.json").exists()
    assert (cache_local_path / "input.json").exists()
    assert (cache_local_path / "meta.json").exists()


@pytest.mark.usefixtures("set_home")
def test_skip_breakpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the breakpoint skip.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    pipeline_name = "pipeline"
    pipeline_hash = "xyz"
    pipeline_path = (
        tmp_path / ".sdag" / "pipelines" / pipeline_name / pipeline_hash
    )
    pipeline_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "sys.argv", ["sdag", "skip", pipeline_name, "--hash", pipeline_hash]
    )
    cli()
    assert pipeline_path.joinpath("skip.lock").is_file()


@pytest.mark.usefixtures("set_home")
def test_continue_breakpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test continue breakpoint.

    Args:
        tmp_path (Path): Temporary path fixture.
        monkeypatch (pytest.MonkeyPatch): Patcher.
    """
    pipeline_name = "pipeline"
    pipeline_hash = "xyz"
    pipeline_path = (
        tmp_path / ".sdag" / "pipelines" / pipeline_name / pipeline_hash
    )
    pipeline_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "sys.argv",
        ["sdag", "continue", pipeline_name, "--hash", pipeline_hash],
    )
    cli()
    assert pipeline_path.joinpath("continue.lock").is_file()
