"""Settings."""

import logging
import sys
from functools import lru_cache
from pathlib import Path

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from strenum import StrEnum

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Env variables set by the scheduler.

    Attributes:
        sdag_pipeline_dir (str): Pipeline directory.
        sdag_pipeline_name (str): Pipeline name.
        sdag_subpipeline_name (str): Task pipeline name.
            If there are no nested pipeline, it is equal
            to the pipeline name.
        sdag_import_path (str): Import path used to compile
            the pipeline.
        sdag_input_kwargs (str): Pipeline input kwargs
            JSON string. Useful to recompile the pipeline.
        sdag_uid (str): Node unique id.
        sdag_task_fn (str): Task function name.
        sdag_task_name (str): Task name, by default it's equal to
            the task name.
        sdag_try_num (int): Number of times the task has been executed.
    """

    sdag_pipeline_dir: Path
    sdag_pipeline_name: str
    sdag_subpipeline_name: str
    sdag_import_path: str
    sdag_input_kwargs: str
    sdag_uid: int
    sdag_task_name: str
    sdag_task_fn: str
    sdag_try_num: int
    sdag_log_level: str


@lru_cache
def get_cached_settings() -> Settings:
    """Get the SDAG environment variables.

    Returns:
        Settings: Parsed environment variables.
    """
    return Settings()  # type: ignore


class CompileSettings(BaseSettings):
    """Compilation environment variables.

    Attributes:
        sdag_base_path (Path | None): Base path. Defaults to None.
    """

    sdag_base_path: Path | None = None


@lru_cache
def get_compile_settings() -> CompileSettings:
    """Get cached compilation settings.

    Returns:
        CompileSettings: Cached compilation settings.
    """
    return CompileSettings()


class LogColor(StrEnum):
    """Logging colors matching with env_logger."""

    BLUE = "\033[34m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    RESET = "\033[0m"


class EnvLoggerFormatter(logging.Formatter):
    """Formatter mimicking env_logger.

    Attributes:
        _crop (dict[str, str]): Used to crop log levels as
            env_logger does.
        _colors (dict[str, str]): Logging level colors.
    """

    def __init__(self):
        """Initialize the log formatter."""
        self._crop = {"WARNING": "WARN", "CRITICAL": "CRIT"}
        self._colors = {
            "DEBUG": LogColor.BLUE,
            "INFO": LogColor.GREEN,
            "WARN": LogColor.YELLOW,
            "ERROR": LogColor.RED,
            "CRIT": LogColor.RED,
        }
        fmt = "[%(asctime)s %(levelname)-5s %(name)s] %(message)s"
        super().__init__(fmt=fmt, datefmt="%Y-%m-%dT%H:%M:%SZ")

    def format(self, record: logging.LogRecord) -> str:
        """Format a record.

        Args:
            record (logging.LogRecord): Raw record.

        Returns:
            str: Formatted record.
        """
        record.levelname = self._format_levelname(record.levelname)
        return super().format(record)

    def _format_levelname(self, levelname: str) -> str:
        """Format the level name like env_logger does.

        The level name is cropped or padded to 5 characters
        and colored.

        Args:
            levelname (str): Levelname.

        Returns:
            str: Formatted levelname.
        """
        name = self._crop.get(levelname, levelname)
        color = self._colors.get(name, "INFO")
        return f"{color}{name:<5}{LogColor.RESET}"


def configure_logging(log_level: str) -> None:
    """Simple logging configuration.

    Everything is streamed to the stderr.

    Args:
        log_level (str): Logging level.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(EnvLoggerFormatter())
    logging.basicConfig(level=log_level.upper(), handlers=[handler])
