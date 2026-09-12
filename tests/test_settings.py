from pathlib import Path

import pytest
from sdag.settings import (
    EnvLoggerFormatter,
    configure_logging,
    get_log_level,
    parse_pyproject,
)


def test_configure_logging() -> None:
    """Check nothing weird shows up."""
    configure_logging(log_level="info")


@pytest.mark.parametrize(
    argnames=("log_level_in", "log_level_out"),
    argvalues=[(None, "info"), ("debug", "debug")],
)
def test_get_log_level(log_level_in: str | None, log_level_out: str) -> None:
    assert get_log_level(log_level_in) == log_level_out


def test_parse_pyproject(tmp_path: Path) -> None:
    content = """[tool.sdag]
    dag-dir = "path/to/dagdir"
    compiled-dag-dir = "path/to/compiled"
    prepend-compiled-dag-dir = true
    log-level = "debug"
    cmd = "bash"

    [[tool.sdag.tags]]
    tag = "online"
    cmd = "sbatch"
    """
    path = tmp_path / "pyproject.toml"
    with path.open("w") as f:
        f.write(content)

    pyproj = parse_pyproject(str(path))
    assert pyproj.dag_dir == "path/to/dagdir"
    assert pyproj.compiled_dag_dir == "path/to/compiled"
    assert pyproj.prepend_compiled_dag_dir
    assert pyproj.cmd == "bash"
    assert pyproj.log_level == "debug"
    assert pyproj.tags[0].tag == "online"
    assert pyproj.tags[0].cmd == "sbatch"


class TestEnvLoggerFormatter:
    @pytest.fixture
    def formatter(self) -> EnvLoggerFormatter:
        return EnvLoggerFormatter()

    @pytest.mark.parametrize(
        argnames=("levelname", "formatted"),
        argvalues=[
            ("DEBUG", "\033[34mDEBUG\033[0m"),
            ("INFO", "\033[32mINFO \033[0m"),
            ("WARNING", "\033[33mWARN \033[0m"),
            ("ERROR", "\033[31mERROR\033[0m"),
        ],
    )
    def test_format_logname(
        self, levelname: str, formatted: str, formatter: EnvLoggerFormatter
    ) -> None:
        assert formatter._format_levelname(levelname) == formatted
