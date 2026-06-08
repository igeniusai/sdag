"""CLI commands.

Commands executed through the CLI. It's the point
of contact between the Python API and the Rust core.
"""

import json
import logging
from argparse import Namespace
from pathlib import Path
from typing import Any

import sdag.core as core
from sdag.compiler import master
from sdag.discovery import find_all_pipelines, find_pipeline_by_name
from sdag.exceptions import DAGNotFoundError
from sdag.models import DAG, Kwarg, TaskNode
from sdag.settings import parse_pyproject
from sdag.visualization import MermaidGenerator
from sdag.wrappers import Task

logger = logging.getLogger(__name__)


def _find_cacheable_tasks(
    dag: DAG, local: bool
) -> tuple[list[str], list[str]]:
    """Find tasks with caching enabled.

    Args:
        dag (DAG): DAG.
        local (bool): Set to True to select locally cached tasks,
            otherwise set to False.

    Returns:
        tuple[list[str], list[str]]: Cacheable tasks and their
            pipelines.
    """
    tasks: list[str] = []
    pipelines: list[str] = []
    for node in dag.nodes:
        if node.kind == "task":
            cacheable = node.cache_local if local else node.cache
            if cacheable:
                tasks.append(node.name)
                pipelines.append(node.pipeline_name)

    return tasks, pipelines


def compile_and_return_dag(
    path: str, extra_metadata: Any, input_kwargs: dict[str, Any]
) -> DAG:
    """Compile a pipeline.

    Args:
        path (str): Pipeline name or import string.
        extra_metadata (Any): Extra metadata that will
            be attached to the pipeline.
        input_kwargs (dict[str, Any]): Pipeline input kwargs.

    Returns:
        DAG: Compiled graph.
    """
    logger.info("Compiling pipeline '%s'", path)
    pipeline = find_pipeline_by_name(path)
    dag = pipeline.compile(path, **input_kwargs)
    dag.meta.kwargs |= input_kwargs
    if extra_metadata is not None:
        dag.meta.extra = json.loads(extra_metadata)

    logger.info("Compiled pipeline '%s' with hash '%s'", path, dag.meta.hash)

    return dag


def _find_compiled_path(path: str) -> Path:
    """Find the JSON file path.

    The pyproject settings are used if the path is relative.

    Args:
        path (str): JSON file path.

    Returns:
        Path: Path used to read the compiled pipeline.
    """
    if "/" in path:
        return Path(path)
    pyproj = parse_pyproject()
    if not pyproj.prepend_compiled_dag_dir:
        return Path(path)
    return Path(pyproj.compiled_dag_dir) / path


def _parse_compiled_pipeline(path: str) -> DAG:
    """Parse a compiled JSON.

    Args:
        path (str): User-selected path.

    Returns:
        DAG: Parsed pipeline.
    """
    compiled_path = _find_compiled_path(path)
    with compiled_path.open() as f:
        data = json.load(f)
    return DAG.model_validate(data, by_alias=True)


def _get_dag_from_name_import_or_json(
    path: str, extra_metadata: Any, input_kwargs: dict[str, Any]
) -> DAG:
    """Flexible way to import a pipeline.

    Args:
        path (str): Pipeline name or import string or JSON path.
        extra_metadata (Any): Extra metadata, only relevant if
            the pipeline is not read from a JSON.
        input_kwargs (dict[str, Any]): Pipeline input kwargs,
            only relevant if the pipeline is not read from
            a JSON.

    Returns:
        DAG: Parsed or compiled pipeline.
    """
    if path.endswith(".json"):
        return _parse_compiled_pipeline(path)

    return compile_and_return_dag(
        path=path, extra_metadata=extra_metadata, input_kwargs=input_kwargs
    )


def get_task(name: str, pipeline_name: str) -> Task:
    """Import a task.

    Args:
        name (str): Task name.
        pipeline_name (str): Name of the pipeline using the task.

    Raises:
        DAGNotFoundError: The task cannot be found.

    Returns:
        Task: Task.
    """
    pipeline = find_pipeline_by_name(pipeline_name)
    if name in pipeline.tasks:
        return pipeline.tasks[name]
    if name in master.tasks:
        return master.tasks[name]

    msg = f"Task '{name}' of pipeline '{pipeline_name}' not found"
    raise DAGNotFoundError(msg)


def compile_pipeline(args: Namespace, extras: dict[str, Any]) -> Path:
    """Compile a pipeline.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra args turned into input kwargs.

    Returns:
        Path: Path where the compiled JSON has been saved.
    """
    dag = compile_and_return_dag(
        path=args.pipeline,
        extra_metadata=args.extra_metadata,
        input_kwargs=extras,
    )
    dag_json = dag.model_dump_json(indent=4, warnings="none", by_alias=True)

    dst_dir = args.dst_dir
    if dst_dir is None:
        pyproj = parse_pyproject()
        dst_dir = pyproj.compiled_dag_dir

    dst_path = Path(dst_dir)
    dst_path.mkdir(parents=True, exist_ok=True)
    dag_name = dag.meta.pipeline_name
    dst_name = args.name if args.name is not None else f"{dag_name}.json"

    dst_path = dst_path / dst_name
    logger.info("Saving pipeline to '%s'", dst_path)

    with dst_path.open("w") as f:
        f.write(dag_json)

    return dst_path


def run_pipeline(args: Namespace, extras: dict[str, Any]) -> None:
    """Run a pipeline.

    Args:
        args (Namespace): Parsed arguments.
        extras (dict[str, Any]): Extra arguments only used
            if the pipeline must be compiled.
    """
    if args.pipeline.endswith(".json"):
        path = _find_compiled_path(args.pipeline)
    else:
        path = compile_pipeline(args, extras)

    core.run(
        pipeline_path=str(path),
        max_concurrency=args.max_concurrency,
        time_between_polls=args.time_between_polls,
        log_level=args.log_level,
        local=args.local,
        debug=args.debug,
    )


def restart_run(args: Namespace, extras: dict[str, Any]) -> None:
    """Restart a run.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra args (not allowed).

    Raises:
        ValueError: Extra argument used.
    """
    if extras:
        raise ValueError

    if args.pipeline.endswith(".json"):
        dag = _parse_compiled_pipeline(args.pipeline)
        pipeline_name = dag.meta.pipeline_name
        pipeline_hash = dag.meta.hash

    else:
        pipeline_name = args.pipeline
        pipeline_hash = args.hash

    core.restart_run(
        pipeline_name=pipeline_name,
        pipeline_hash=pipeline_hash,
        max_concurrency=args.max_concurrency,
        time_between_polls=args.time_between_polls,
        log_level=args.log_level,
    )


def kill_pipeline(args: Namespace, extras: dict[str, Any]) -> None:
    """Kill a pipeline.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra args (not allowed).

    Raises:
        ValueError: Extra argument used.
    """
    if extras:
        raise ValueError

    if args.pipeline.endswith(".json"):
        dag = _parse_compiled_pipeline(args.pipeline)
        pipeline_name = dag.meta.pipeline_name
        pipeline_hash = dag.meta.hash

    else:
        pipeline_name = args.pipeline
        pipeline_hash = args.hash

    core.kill_run(
        pipeline_name=pipeline_name,
        pipeline_hash=pipeline_hash,
        log_level=args.log_level,
    )


def prune_cache(args: Namespace, extras: dict[str, Any]) -> None:
    """Prune the cache of tasks or pipelines.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra args (not allowed).

    Raises:
        ValueError: Extra argument used.
    """
    if args.pipeline is None and not args.task:
        raise ValueError

    if args.pipeline is not None and not args.task:
        dag = _get_dag_from_name_import_or_json(
            path=args.pipeline,
            extra_metadata=args.extra_metadata,
            input_kwargs=extras,
        )

        tasks, _ = _find_cacheable_tasks(dag, local=False)
        for task in tasks:
            core.prune_cache(
                task_name=task,
                pipeline_name=None,
                allow_full_prune=False,
                log_level=args.log_level,
            )

        tasks, pipelines = _find_cacheable_tasks(dag, local=True)
        for task, pipeline in zip(tasks, pipelines, strict=True):
            core.prune_cache(
                task_name=task,
                pipeline_name=pipeline,
                allow_full_prune=False,
                log_level=args.log_level,
            )

    else:
        name = args.pipeline
        if name is not None and ":" in name:
            name = name.split(":")[-1]

        for task in args.task:
            core.prune_cache(
                task_name=task,
                pipeline_name=name,
                allow_full_prune=True,
                log_level=args.log_level,
            )


def view_pipeline(args: Namespace, extras: dict[str, Any]) -> None:
    """Visualize a pipeline in the terminal.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra arguments only used
            if the pipeline is not read from JSON.
    """
    dag = _get_dag_from_name_import_or_json(
        path=args.pipeline,
        extra_metadata=args.extra_metadata,
        input_kwargs=extras,
    )

    mermaid_gen = MermaidGenerator(squeeze=args.squeeze)
    mermaid = mermaid_gen.generate_mermaid_string(dag)
    core.view_pipeline(mermaid=mermaid, log_level=args.log_level)


def run_task(args: Namespace, extras: dict[str, Any]) -> None:
    """Run a single task without starting the scheduler.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra arguments used for compilation.
    """
    task = get_task(name=args.task, pipeline_name=args.pipeline)
    if args.local:
        task.cmd = "bash"

    task = TaskNode(
        uid=0,
        root_uid=-1,
        name=task.name,
        pipeline_name=args.pipeline,
        fn_name=task.fn.__name__,
        cache=task.cache,
        cache_local=task.cache_local,
        debug=False,
        mode=task.mode,
        cmd=task.cmd,
        retries=0,
        script=task.script,
        kwargs=[Kwarg(key=k, value=v) for k, v in extras.items()],
    )

    task_serialized = task.model_dump_json(warnings="none", by_alias=True)
    core.run_single_task(task_serialized, log_level=args.log_level)


def list_pipelines(args: Namespace, extras: dict[str, Any]) -> None:
    """List pipelines and runs.

    By default, pipelines are listed. If the pipeline name is
    specified, then run hashes are listed.


    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra arguments (not allowed)

    Raises:
        ValueError: Extra argument used.
    """
    if extras:
        raise ValueError

    if args.pipeline is not None:
        core.print_runs(pipeline_name=args.pipeline, log_level=args.log_level)
        return

    pipelines = find_all_pipelines()
    names = list(pipelines)
    paths = list(pipelines.values())
    core.print_pipeline_list(names, paths)


def describe_pipeline(args: Namespace, extras: dict[str, Any]) -> None:
    """Describe a pipeline arguments, caching, and artifacts.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra arguments used for compiling.
    """
    if args.pipeline.endswith(".json"):
        path = _find_compiled_path(args.pipeline)
    else:
        path = compile_pipeline(args, extras)

    core.describe_pipeline(str(path), log_level=args.log_level)


def skip_breakpoint(args: Namespace, extras: dict[str, Any]) -> None:
    """Skip a breakpoint and mark the task as failed.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra args (not allowed).

    Raises:
        ValueError: Extra argument used.
    """
    if extras:
        raise ValueError

    if args.pipeline.endswith(".json"):
        dag = _parse_compiled_pipeline(args.pipeline)
        pipeline_name = dag.meta.pipeline_name
        pipeline_hash = dag.meta.hash

    else:
        pipeline_name = args.pipeline
        pipeline_hash = args.hash

    core.skip_breakpoint(
        pipeline_name=pipeline_name,
        pipeline_hash=pipeline_hash,
        log_level=args.log_level,
    )


def continue_breakpoint(args: Namespace, extras: dict[str, Any]) -> None:
    """Reset the task and unlock the scheduler.

    Args:
        args (Namespace): Parsed args.
        extras (dict[str, Any]): Extra arguments (not allowed).

    Raises:
        ValueError: Extra argument used.
    """
    if extras:
        raise ValueError

    if args.pipeline.endswith(".json"):
        dag = _parse_compiled_pipeline(args.pipeline)
        pipeline_name = dag.meta.pipeline_name
        pipeline_hash = dag.meta.hash

    else:
        pipeline_name = args.pipeline
        pipeline_hash = args.hash

    core.continue_breakpoint(
        pipeline_name=pipeline_name,
        pipeline_hash=pipeline_hash,
        log_level=args.log_level,
    )
