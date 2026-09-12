"""Settings."""

import logging
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

import tomllib
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings

from sdag.models import Commands


class SDAGTag(BaseModel):
    """Tag configurations.

    Attributes:
        tag (str): Tag name.
        cmd (Commands | None): Command. Defaults to None.
    """

    tag: str
    cmd: Commands | None = None


class Pyproj(BaseModel):
    """pyproject.toml configs.

    Attributes:
        dag_dir (str): DAG directory.
        compiled_dag_dir (str): Compiled DAG directory.
        prepend_compiled_dag_dir (bool): Add the compiled pipeline
            dir automatically if the directory has not been
            specified.
        commands (Commands | None): Commands to execute tasks. If
            set, it overrides all pipeline commands.
        log_level (Literal["debug", "info", "warning", "error"]):
            Logging level.
    """

    dag_dir: str = Field(alias="dag-dir", default="./pipelines")
    compiled_dag_dir: str = Field(alias="compiled-dag-dir", default=".")
    cmd: Commands | None = None
    prepend_compiled_dag_dir: bool = Field(
        alias="prepend-compiled-dag-dir", default=False
    )
    log_level: Literal["debug", "info", "warning", "error"] = Field(
        alias="log-level", default="info"
    )
    tags: list[SDAGTag] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


@lru_cache
def parse_pyproject(pyproj_path: str = "pyproject.toml") -> Pyproj:
    """Parse and cache di pyproject.toml.

    Args:
        pyproj_path (str, optional): pyproject.toml path.
            Defaults to "pyproject.toml".

    Returns:
        Pyproj: Parsed configs.
    """
    path = Path(pyproj_path)
    if not path.is_file():
        return Pyproj()

    with path.open("rb") as f:
        pyproj = tomllib.load(f)

    if "tool" not in pyproj:
        return Pyproj()

    tool_sdag = pyproj["tool"].get("sdag")
    if tool_sdag is None:
        return Pyproj()

    return Pyproj.model_validate(tool_sdag)


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
        sdag_try_num: Number of times the task has been executed.
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


def get_log_level(log_level: str | None = None) -> str:
    """Get the sdag log level.

    If not set by the user, it is read from the pyproject.toml.

    Args:
        log_level (str | None, optional): Logging level.
            Defaults to None.

    Returns:
        str: Log level.
    """
    if log_level is None:
        pyproj = parse_pyproject()
        log_level = pyproj.log_level
    return log_level


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
