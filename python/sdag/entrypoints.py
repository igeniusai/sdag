"""sdag entrypoints."""

import logging

from sdag.exceptions import CLIError
from sdag.settings import configure_logging, get_log_level

logger = logging.getLogger(__name__)


def cli():
    """CLI entrypoint."""
    from sdag.parser import ExtraArgumentParser, ParserBuilder

    parser = (
        ParserBuilder()
        .add_version()
        .add_compile_subparser()
        .add_run_subparser()
        .add_restart_subparser()
        .add_kill_subparser()
        .add_prune_subparser()
        .add_runtask_subparser()
        .add_view_subparser()
        .add_list_subparser()
        .add_describe_subparser()
        .add_skip_subparser()
        .add_continue_subparser()
        .get_parser()
    )

    args, extras = parser.parse_known_args()
    extra_parser = ExtraArgumentParser()
    extra_args = extra_parser.parse(extras)
    if "log_level" not in args or "fn" not in args:
        raise CLIError

    args.log_level = get_log_level(args.log_level)
    configure_logging(args.log_level)
    args.fn(args, extra_args)


def sdag_execute(configure_logger: bool = True) -> None:  # noqa: FBT002
    """Task entry point.

    This is the function called by script to run wrapping tasks.
    It is also possible to use the `sdag-execute` command.

    Args:
        configure_logger (bool): Whether to configure the default
            logger. Defaults to True.
    """
    import inspect
    import json

    from sdag.io import IOHandler, find_and_import_task
    from sdag.settings import get_cached_settings

    if configure_logger:
        log_level = get_log_level()
        configure_logging(log_level)

    settings = get_cached_settings()
    logger.info(
        "Task: '%s'\nFunction: '%s'\nNode: '%s'"
        "\nPipeline: '%s'\nTry number: '%s'",
        settings.sdag_task_name,
        settings.sdag_task_fn,
        settings.sdag_uid,
        settings.sdag_subpipeline_name,
        settings.sdag_try_num,
    )

    handler = IOHandler(
        uid=settings.sdag_uid, pipeline_dir=settings.sdag_pipeline_dir
    )

    task = find_and_import_task(
        task_name=settings.sdag_task_fn,
        pipeline_name=settings.sdag_pipeline_name,
        subpipeline_name=settings.sdag_subpipeline_name,
        import_path=settings.sdag_import_path,
        input_kwargs=settings.sdag_input_kwargs,
    )
    logger.info("Found task '%s'", task.name)
    sig = inspect.signature(task.fn)
    input_kwargs = handler.get_input(sig)
    logger.info("Input values:\n%s", json.dumps(input_kwargs, indent=4))

    artifacts = handler.get_artifacts(sig, input_kwargs)
    logger.info("Artifacts:\n%s", handler.serialize_artifacts(artifacts))

    handler.cast_values(sig, input_kwargs)
    output = task.fn(**input_kwargs)
    logger.info("Output values:\n%s", json.dumps(output, indent=4))

    handler.serialize_output(output, artifacts)
    logger.info("Task '%s' completed", settings.sdag_task_name)
