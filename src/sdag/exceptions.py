"""Custom exceptions."""


class SDAGError(Exception):
    """Base exception, can be used to catch them all."""


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


class RootNotFoundError(SDAGError):
    """Root node not found."""

    def __init__(self):
        """Raise the exception."""
        msg = "Root node not found."
        super().__init__(msg)


class EndNotFoundError(SDAGError):
    """End node not found."""

    def __init__(self):
        """Raise the exception."""
        msg = "End node not found."
        super().__init__(msg)


class TaskNotUniqueError(SDAGError):
    """Task name is not unique."""

    def __init__(self, name: str):
        """Raise the error.

        Args:
            name (str): Not unique task name.
        """
        msg = f"Task name '{name}' is not unique."
        super().__init__(msg)


class DAGNotSetError(SDAGError):
    """Raised when registering stuff without a pipeline set.

    DAG can be None, so it must be checked everytime it is accessed.
    """

    def __init__(self):
        """Raise the exception."""
        msg = "Target DAG is not set."
        super().__init__(msg)


class MissingActiveBranchError(SDAGError):
    """Active branch is missing."""

    def __init__(self):
        """Raise the exception."""
        msg = "No current active branch."
        super().__init__(msg)


class IncorrectElseError(SDAGError):
    """Else is incorrectly used."""

    def __init__(self):
        """Raise the exception."""
        msg = "Else statement incorrectly used."
        super().__init__(msg)


class IncorrectElifError(SDAGError):
    """Elif is incorrectly used."""

    def __init__(self):
        """Raise the exception."""
        msg = "Elif statement incorrectly used."
        super().__init__(msg)
