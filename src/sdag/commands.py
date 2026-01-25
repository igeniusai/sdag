"""CLI commands."""

import logging
from argparse import Namespace
from pathlib import Path
from typing import Any

from sdag.wrappers import Pipeline

logger = logging.getLogger(__name__)


def compile_pipeline(args: Namespace) -> Path:
    """Compile a pipeline.

    Args:
        args (Namespace): CLI arguments.

    Returns:
        Path: Compiled pipeline path.
    """
    from sdag.sdag import sdag

    logger.info("Start compiling the pipeline.")

    pipeline = _import_pipeline(args.pipeline)
    extra = _parse_json_input(args.extra_metadata)
    input_kwargs = _parse_json_input(args.input_kwargs)
    path = sdag.compile(
        pipeline=pipeline,
        dst_dir=args.dst_dir,
        name=args.name,
        optimize=args.optimize,
        input_kwargs=input_kwargs,
        extra_metadata=extra,
    )
    logger.debug("Pipeline compiled to '%s'", path)
    return path


def run_pipeline(args: Namespace) -> None:
    """Run a compiled pipeline.

    Args:
        args (Namespace): CLI arguments.
    """
    import sscheduler

    logger.info("Starting the scheduler")

    sscheduler.start_scheduler(  # type: ignore
        pipeline=args.pipeline,
        wait_seconds=args.wait_seconds,
        max_concurrency=args.max_concurrency,
        log_level=args.log_level,
        local=args.local,
        restart=args.restart,
    )


def compile_and_run_pipeline(args: Namespace) -> None:
    """Compile and run a pipeline.

    Args:
        args (Namespace): CLI arguments.
    """
    import sscheduler

    logger.info("Start compiling and running the pipeline")
    path = compile_pipeline(args)
    sscheduler.start_scheduler(  # type: ignore
        pipeline=str(path),
        wait_seconds=args.wait_seconds,
        max_concurrency=args.max_concurrency,
        log_level=args.log_level,
        local=args.local,
        restart=args.restart,
    )


def kill_pipeline(args: Namespace) -> None:
    """Kill all running tasks of a pipeline.

    Args:
        args (Namespace): CLI arguments.
    """
    import sscheduler

    sscheduler.kill_pipeline(  # type: ignore
        pipeline=args.pipeline,
        log_level=args.log_level,
    )


def prune_cache(args: Namespace) -> None:
    """Clear the cache.

    if the task name has been set to 'all' and a task named
    'all' does not exist, then the whole cache will be cleared.

    Args:
        args (Namespace): CLI arguments.
    """
    import sscheduler

    sscheduler.prune_cache(  # type: ignore
        task=args.task,
        log_level=args.log_level,
    )


def _import_pipeline(import_str: str) -> Pipeline:
    """Dynamically import a pipeline.

    Args:
        import_str (str): Import string in the form
            `path.to.module:pipeline_fn`

    Returns:
        Pipeline: Pipeline.
    """
    from importlib import import_module

    path, pipeline_fn = import_str.split(":")
    mod = import_module(path)
    return getattr(mod, pipeline_fn)


def _parse_json_input(data: str | None) -> Any:
    """Parse an input JSON string.

    Args:
        data (str | None): JSON string.

    Raises:
        Exception: The JSON string parsing fails.

    Returns:
        Any: Parsed JSON.
    """
    import json

    if data is None:
        return data

    try:
        return json.loads(data)
    except Exception:
        logger.exception("Failed to parse json string data '%s'", data)
        raise
