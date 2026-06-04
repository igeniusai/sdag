"""Custom exceptions."""

from sdag.constants import POSIX_ENV_VARIABLES


class SDAGError(Exception):
    """SDAG exception base class."""


class TaskNotUniqueError(SDAGError):
    """Task name is not unique."""

    def __init__(self, name: str):
        """Raise the error.

        Args:
            name (str): Not unique task name.
        """
        msg = f"Task name '{name}' is not unique."
        super().__init__(msg)


class KwargNotFoundError(SDAGError):
    """Task input kwarg not found."""

    def __init__(self, key: str):
        """Raise the error.

        Args:
            key (str): Input key not found.
        """
        msg = f"Input argument '{key}' not found."
        super().__init__(msg)


class DynamicArtifactError(SDAGError):
    """Dynamic artifacts are not allowed."""

    def __init__(self, key: str):
        """Raise the error.

        Args:
            key (str): Input key not found.
        """
        msg = f"key '{key}': Dynamic artifacts are not allowed."
        super().__init__(msg)


class DAGNotSetError(SDAGError):
    """No DAG is set in the compiler."""

    def __init__(self):
        """Raise the exception."""
        msg = "no pipelines are currently being compiled."
        super().__init__(msg)


class IncorrectElifError(SDAGError):
    """Incorrect Elif expression."""

    def __init__(self):
        """Raise the exception."""
        msg = "Incorrect Elif expression."
        super().__init__(msg)


class IncorrectElseError(SDAGError):
    """Incorrect else expression."""

    def __init__(self):
        """Raise the exception."""
        msg = "Incorrect Else expression."
        super().__init__(msg)


class POSIXOverrideError(SDAGError):
    """External tasks have variables conflicting with POSX."""

    def __init__(self, task_name: str, key: str):
        """Raise the exception.

        Args:
            task_name (str): Task violating the check.
            key (str): Forbidden key used.
        """
        banned = ", ".join(POSIX_ENV_VARIABLES)
        key_up = key.upper()
        msg = (
            f"External task '{task_name}' is trying to set the '{key}'"
            f" keyword argument. Please choose a different name as '{key_up}'"
            " would override a standard POSIX environment variable."
            f" The banned keywords are:\n{banned}"
        )
        super().__init__(msg)


class CLIError(SDAGError):
    """CLI error."""

    def __init__(self):
        """Raise the exception."""
        msg = "Unknown CLI command."
        super().__init__(msg)


class DAGNotFoundError(SDAGError):
    """DAG not found in path."""
