"""sdag parser."""

import sys
from argparse import ArgumentParser, BooleanOptionalAction

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sdag._version import __version__
from sdag.commands import (
    compile_and_run_pipeline,
    compile_pipeline,
    kill_pipeline,
    prune_cache,
    run_pipeline,
)


class ParserBuilder:
    """Build the parser.

    Attributes:
        _parser (ArgumentParser): Main parser.
        _subparsers (_SubParsersAction[ArgumentParser]):
            Subparsers for the different commands.
    """

    def __init__(self):
        """Initialize the parser."""
        self._parser = ArgumentParser(description="SDAG")
        self._subparsers = self._parser.add_subparsers(
            help="Available commands"
        )

    def add_version(self) -> Self:
        """Add versions.

        Returns:
            Self: Parser builder.
        """
        self._parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=f"%(prog)s {__version__}",
        )

        return self

    def add_compile_subparser(self) -> Self:
        """Add the compile command.

        Returns:
            Self: Parser builder.
        """
        compile_parser = self._subparsers.add_parser(
            "compile", help="Compile a pipeline."
        )

        self._add_pipeline_import_str(compile_parser)
        self._add_compilation_args(compile_parser)
        compile_parser.set_defaults(fn=compile_pipeline)

        return self

    def add_run_subparser(self) -> Self:
        """Add the run command.

        Returns:
            Self: Parser builder.
        """
        run_parser = self._subparsers.add_parser(
            "run", help="Run a compiled pipeline."
        )
        run_parser.add_argument(
            "pipeline",
            type=str,
            help="Path to the compiled JSON file.",
        )

        self._add_log_level(run_parser)
        self._add_scheduler_args(run_parser)
        run_parser.set_defaults(fn=run_pipeline)

        return self

    def add_compile_run_subparser(self) -> Self:
        """Add the compile-and-run (cr) command.

        Returns:
            Self: Parser builder.
        """
        cr_parser = self._subparsers.add_parser(
            "cr", help="Compile and run a pipeline."
        )

        self._add_pipeline_import_str(cr_parser)
        self._add_log_level(cr_parser)
        self._add_compilation_args(cr_parser)
        self._add_scheduler_args(cr_parser)
        cr_parser.set_defaults(fn=compile_and_run_pipeline)

        return self

    def add_kill_subparser(self) -> Self:
        """Add the pipeline kill command.

        Returns:
            Self: Parser builder.
        """
        kill_parser = self._subparsers.add_parser(
            "kill", help="Kill the runnig tasks of a pipeline."
        )
        kill_parser.add_argument(
            "pipeline",
            type=str,
            help="pipeline function name",
        )

        self._add_log_level(kill_parser)
        kill_parser.set_defaults(fn=kill_pipeline)

        return self

    def add_prune_subparser(self) -> Self:
        """Add the cache prune command.

        Returns:
            Self: Parser builder.
        """
        prune_parser = self._subparsers.add_parser(
            "prune", help="Prune the cache of a task."
        )
        prune_parser.add_argument(
            "task",
            type=str,
            help=(
                "Task to be pruned from the cache. Set to"
                " 'all' to prune the whole cache."
            ),
        )

        self._add_log_level(prune_parser)
        prune_parser.set_defaults(fn=prune_cache)

        return self

    def get_parser(self) -> ArgumentParser:
        """Get the parser.

        Returns:
            ArgumentParser: Parser.
        """
        return self._parser

    def _add_log_level(self, subparser: ArgumentParser) -> Self:
        """Add the scheduler log level option.

        Args:
            subparser (ArgumentParser): Subparser.

        Returns:
            Self: Parser builder.
        """
        subparser.add_argument(
            "-l",
            "--log-level",
            choices=["debug", "info", "warning", "error"],
            default="info",
            help="Scheduler log level. Defaults to 'info'",
        )

        return self

    def _add_pipeline_import_str(self, subparser: ArgumentParser) -> Self:
        """Add the pipeline import string option.

        Args:
            subparser (ArgumentParser): Subparser.

        Returns:
            Self: Parser builder.
        """
        subparser.add_argument(
            "pipeline",
            type=str,
            help="Pipeline import string (e.g., 'path.to:pipeline_fn').",
        )

        return self

    def _add_scheduler_args(self, subparser: ArgumentParser) -> Self:
        """Add the scheduler options.

        Args:
            subparser (ArgumentParser): Subparser.

        Returns:
            Self: Parser builder.
        """
        subparser.add_argument(
            "-w",
            "--wait-seconds",
            type=int,
            default=5,
            help="Time between slurm polls (seconds). Defaults to 5s.",
        )

        subparser.add_argument(
            "-m",
            "--max-concurrency",
            type=int,
            default=10,
            help=(
                "Maximum number of jobs running at the same time."
                "Set to 0 for unlimited concurrency. Defaults to 10."
            ),
        )

        subparser.add_argument(
            "-r",
            "--restart",
            action=BooleanOptionalAction,
            default=False,
            help="Restart a pipeline from the last checkpoint.",
        )

        subparser.add_argument(
            "--local",
            action=BooleanOptionalAction,
            default=False,
            help=(
                "Run tasks locally as blocking, child processes by"
                " executing the script with bash."
            ),
        )

        return self

    def _add_compilation_args(self, subparser: ArgumentParser) -> Self:
        """Add the compilation options.

        Args:
            subparser (ArgumentParser): Subparser.

        Returns:
            Self: Parser builder.
        """
        subparser.add_argument(
            "-d",
            "--dst-dir",
            type=str,
            default=None,
            help="Destination directory. Defaults to the current one.",
        )

        subparser.add_argument(
            "-n",
            "--name",
            type=str,
            default=None,
            help="Destination directory. Defaults to the current one.",
        )

        subparser.add_argument(
            "-i",
            "--input-kwargs",
            type=str,
            default=None,
            help="Input kwargs as a JSON string.",
        )

        subparser.add_argument(
            "-e",
            "--extra-metadata",
            type=str,
            default=None,
            help="Extra metadata as a JSON string.",
        )

        subparser.add_argument(
            "-o",
            "--optimize",
            action=BooleanOptionalAction,
            default=False,
            help="Optimize graph.",
        )

        return self
