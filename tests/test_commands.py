from pathlib import Path

import pytest
from sdag.commands import (
    _find_cacheable_tasks,
    _find_compiled_path,
    _get_dag_from_name_import_or_json,
    _parse_compiled_pipeline,
)
from sdag.models import DAG


@pytest.fixture
def dag() -> DAG:
    dag = {
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

    return DAG.model_validate(dag)


@pytest.mark.parametrize(
    argnames=("local", "tasks", "pipelines"),
    argvalues=[
        (False, ["task_global"], ["dag"]),
        (True, ["task_local"], ["dag"]),
    ],
)
def test_find_cacheable_tasks(
    local: bool, tasks: list[str], pipelines: list[str], dag: DAG
) -> None:
    exp_tasks, exp_pipelines = _find_cacheable_tasks(dag, local=local)
    assert exp_tasks == tasks
    assert exp_pipelines == pipelines


def test_find_relative_compiled_path() -> None:
    assert _find_compiled_path("test.json") == Path(
        "compiled-pipelines/test.json"
    )


def test_find_absolute_compiled_path() -> None:
    assert _find_compiled_path("/test.json") == Path("/test.json")


def test_parse_compiled_pipeline(dag: DAG, tmp_path: Path) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    file_path = tmp_path.joinpath("test.json")
    with file_path.open("w") as fin:
        fin.write(dag.model_dump_json(by_alias=True))
    parsed_dag = _parse_compiled_pipeline(str(file_path))
    assert parsed_dag == dag


def test_get_dag_from_name_import_or_json(tmp_path: Path, dag: DAG) -> None:
    tmp_path.mkdir(parents=True, exist_ok=True)
    file_path = tmp_path.joinpath("test.json")
    with file_path.open("w") as fin:
        fin.write(dag.model_dump_json(by_alias=True))

    parsed_dag = _get_dag_from_name_import_or_json(
        str(file_path), extra_metadata=None, input_kwargs={}
    )

    assert parsed_dag == dag
