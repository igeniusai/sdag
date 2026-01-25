"""Test the parser builder."""

import pytest

from sdag.commands import (
    compile_and_run_pipeline,
    compile_pipeline,
    kill_pipeline,
    prune_cache,
    run_pipeline,
)
from sdag.parser import ParserBuilder


class TestParserBuilder:
    """Test the parser builder."""

    @pytest.fixture
    def builder(self) -> ParserBuilder:
        """Empty parser builder.

        Returns:
            ParserBuilder: Parser builder.
        """
        return ParserBuilder()

    def test_compile(self, builder: ParserBuilder) -> None:
        """Test the compile parser.

        Args:
            builder (ParserBuilder): Builder.
        """
        parser = builder.add_compile_subparser().get_parser()
        args = parser.parse_args(
            args=[
                "compile",
                "path.to:pipeline",
                "-d",
                "dir",
                "-n",
                "name",
                "-i",
                '{"a": 2}',
                "-e",
                '{"extra": "extra"}',
            ]
        )

        assert args.pipeline == "path.to:pipeline"
        assert args.dst_dir == "dir"
        assert args.name == "name"
        assert args.input_kwargs == '{"a": 2}'
        assert args.extra_metadata == '{"extra": "extra"}'
        assert not args.optimize
        assert args.fn is compile_pipeline

    def test_run(self, builder: ParserBuilder) -> None:
        """Test the run parser.

        Args:
            builder (ParserBuilder): Builder.
        """
        parser = builder.add_run_subparser().get_parser()
        args = parser.parse_args(
            args=[
                "run",
                "/path/to/json",
                "-l",
                "debug",
                "-w",
                "4",
                "-m",
                "5",
                "--restart",
            ]
        )

        assert args.pipeline == "/path/to/json"
        assert args.log_level == "debug"
        assert args.wait_seconds == 4
        assert args.max_concurrency == 5
        assert args.restart
        assert not args.local
        assert args.fn is run_pipeline

    def test_compile_run(self, builder: ParserBuilder) -> None:
        """Test the cr parser.

        Args:
            builder (ParserBuilder): Parser builder.
        """
        parser = builder.add_compile_run_subparser().get_parser()
        args = parser.parse_args(
            args=[
                "cr",
                "path.to:pipeline",
                "-d",
                "dir",
                "-n",
                "name",
                "-i",
                '{"a": 2}',
                "-e",
                '{"extra": "extra"}',
                "-l",
                "debug",
                "-w",
                "4",
                "-m",
                "5",
                "--restart",
            ]
        )
        assert args.pipeline == "path.to:pipeline"
        assert args.dst_dir == "dir"
        assert args.name == "name"
        assert args.input_kwargs == '{"a": 2}'
        assert args.extra_metadata == '{"extra": "extra"}'
        assert not args.optimize
        assert args.log_level == "debug"
        assert args.wait_seconds == 4
        assert args.max_concurrency == 5
        assert args.restart
        assert not args.local
        assert args.fn is compile_and_run_pipeline

    def test_kill(self, builder: ParserBuilder) -> None:
        """Test the kill parser.

        Args:
            builder (ParserBuilder): Parser builder.
        """
        parser = builder.add_kill_subparser().get_parser()
        args = parser.parse_args(args=["kill", "pipeline", "-l", "debug"])

        assert args.pipeline == "pipeline"
        assert args.log_level == "debug"
        assert args.fn is kill_pipeline

    def test_prune(self, builder: ParserBuilder) -> None:
        """Test the prune parser.

        Args:
            builder (ParserBuilder): Parser builder.
        """
        parser = builder.add_prune_subparser().get_parser()
        args = parser.parse_args(args=["prune", "all", "-l", "debug"])

        assert args.task == "all"
        assert args.log_level == "debug"
        assert args.fn is prune_cache
